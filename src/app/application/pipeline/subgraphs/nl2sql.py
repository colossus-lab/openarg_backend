"""NL2SQL subgraph: generate → execute → fix → fallbacks → format.

Integrated into the query pipeline via
``application/pipeline/connectors/sandbox.py::execute_sandbox_step`` as of
FIX-004 (2026-04-11). The sandbox connector owns **table discovery**
(listing cached tables, resolving planner hints via fnmatch / catalog
search / vector search, year filtering, catalog enrichment). Everything
below that — the retry loop, the last-resort fallback, the INDEC live
fallback, the save-success side effect, and the final DataResult
formatting — lives in this subgraph.

Node topology (see ``specs/010-sandbox-sql/010b-nl2sql/plan.md`` §2):

```
START
  │
  ▼
generate_sql ──▶ execute_sql
                   │
        ┌──────────┼───────────────┐
        │          │               │
     success     retry           last_resort
        │          │               │
        │          ▼               ▼
        │       fix_sql          last_resort_node
        │          │               │
        │          ▼               │
        │       execute_sql        │
        │                          │
        └──────────┬───────────────┘
                   ▼
              format_result
                   │
        ┌──────────┼──────────┐
        │          │          │
  indec_fallback save_success end
        │          │          │
        └──────────┴──────────▶ END
```

The subgraph is compiled at most once per process via
``get_compiled_nl2sql_subgraph()``, which is the only public entry
point for production callers. Unit tests can call
``build_nl2sql_subgraph()`` directly.
"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any

from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from app.application.pipeline._background_tasks import spawn_background
from app.application.pipeline.connectors.cache_table_selection import rewrite_legacy_sql_tables
from app.domain.entities.connectors.data_result import DataResult
from app.domain.ports.llm.llm_provider import LLMMessage
from app.prompts import load_prompt

logger = logging.getLogger(__name__)


class NL2SQLRuntime(TypedDict):
    llm: Any
    sandbox: Any
    embedding: Any
    semantic_cache: Any


_runtime_var: ContextVar[NL2SQLRuntime | None] = ContextVar("nl2sql_runtime", default=None)


@contextmanager
def nl2sql_runtime(
    *,
    llm: Any,
    sandbox: Any,
    embedding: Any,
    semantic_cache: Any,
):
    """Bind request-scoped runtime dependencies outside checkpointed state."""
    token = _runtime_var.set(
        {
            "llm": llm,
            "sandbox": sandbox,
            "embedding": embedding,
            "semantic_cache": semantic_cache,
        }
    )
    try:
        yield
    finally:
        _runtime_var.reset(token)


def _get_runtime() -> NL2SQLRuntime:
    runtime = _runtime_var.get()
    if runtime is None:
        msg = "NL2SQL runtime not initialised — wrap ainvoke() in nl2sql_runtime(...)"
        raise RuntimeError(msg)
    return runtime


def _get_runtime_optional() -> NL2SQLRuntime | None:
    """Return the bound runtime when present, else ``None`` for test fallback."""
    return _runtime_var.get()


class NL2SQLState(TypedDict, total=False):
    """State flowing through the NL2SQL subgraph.

    See ``specs/010-sandbox-sql/010b-nl2sql/plan.md`` §3 for the full
    field inventory and the boundary with the caller. Runtime
    dependencies may be injected either via closure-captured node
    factories (production path) or via state (unit-test path).
    """

    # --- inputs (caller provides) ---
    nl_query: str
    tables: list
    tables_context: str
    table_notes: str
    catalog_entries: dict
    table_descriptions: list
    few_shot_block: str
    max_attempts: int
    # --- working state ---
    generated_sql: str
    result: Any
    attempt: int
    last_error: str
    used_fallback: bool
    indec_pattern_match: bool
    # --- output ---
    data_results: list


def _resolve_runtime_dep(state: NL2SQLState, key: str, runtime_dep: Any) -> Any:
    """Prefer a closure-captured runtime dependency, fall back to state."""
    if runtime_dep is not None:
        return runtime_dep
    if key not in state:
        msg = f"NL2SQL runtime dependency '{key}' not initialised"
        raise RuntimeError(msg)
    return state[key]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _strip_markdown_fences(sql: str) -> str:
    """Strip ``` ```sql ... ``` ``` markdown fences from LLM output."""
    if sql.startswith("```"):
        lines = sql.split("\n")
        lines = [ln for ln in lines if not ln.strip().startswith("```")]
        sql = "\n".join(lines).strip()
    return sql


def _extract_sql(raw: str) -> str:
    """Extract a SELECT/WITH statement from an LLM response.

    Handles: pure SQL, markdown code blocks, text + SQL, multiple statements.
    """
    import re

    text_val = raw.strip()
    code_block = re.search(r"```(?:sql)?\s*\n?(.*?)```", text_val, re.DOTALL | re.IGNORECASE)
    if code_block:
        text_val = code_block.group(1).strip()
    if re.match(r"^\s*(SELECT|WITH)\b", text_val, re.IGNORECASE):
        return text_val
    match = re.search(r"\b((?:WITH|SELECT)\b.*)", text_val, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return text_val


def _compute_indec_match(nl_query: str) -> bool:
    """Internal INDEC pattern check — FR-011 of the spec.

    The caller must NOT pre-compute this; it lives here so the pipeline
    dispatch layer does not have to know about INDEC at all.
    """
    # Lazy import avoids a cycle: connectors/sandbox.py imports this subgraph,
    # and classifiers is a sibling of sandbox.
    from app.application.pipeline.classifiers import INDEC_PATTERN

    return bool(INDEC_PATTERN.search(nl_query))


# ---------------------------------------------------------------------------
# Nodes
# ---------------------------------------------------------------------------


async def generate_sql_node(state: NL2SQLState) -> dict:
    """FR-001: generate SQL from natural language using the LLM.

    Also computes the internal ``indec_pattern_match`` flag (FR-011) so
    downstream edges can decide on the INDEC fallback without the caller
    having to know about it.
    """
    runtime = _get_runtime_optional()
    llm = _resolve_runtime_dep(state, "llm", runtime["llm"] if runtime else None)
    nl_query = state["nl_query"]
    tables_context = state["tables_context"]
    few_shot_block = state.get("few_shot_block", "")

    nl2sql_prompt = load_prompt(
        "nl2sql",
        tables_context=tables_context,
        few_shot_block=few_shot_block or "",
    )

    messages = [
        LLMMessage(role="system", content=nl2sql_prompt),
        LLMMessage(role="user", content=nl_query),
    ]

    llm_response = await llm.chat(
        messages=messages,
        temperature=0.0,
        max_tokens=1024,
    )

    generated_sql = _extract_sql(llm_response.content)
    logger.debug("NL2SQL generated: %s", generated_sql[:200])

    return {
        "generated_sql": generated_sql,
        "attempt": 0,
        "used_fallback": False,
        "indec_pattern_match": _compute_indec_match(nl_query),
    }


async def execute_sql_node(state: NL2SQLState) -> dict:
    """FR-002: execute the generated SQL against the sandbox."""
    runtime = _get_runtime_optional()
    sandbox = _resolve_runtime_dep(state, "sandbox", runtime["sandbox"] if runtime else None)
    generated_sql = state["generated_sql"]
    available_tables = [table.table_name for table in state.get("tables") or []]
    rewritten_sql = rewrite_legacy_sql_tables(generated_sql, available_tables)

    result = await sandbox.execute_readonly(rewritten_sql)

    updates: dict[str, Any] = {"result": result, "generated_sql": rewritten_sql}
    if result.error:
        updates["last_error"] = result.error
    else:
        # Clear the error so a downstream success branch is unambiguous.
        updates["last_error"] = ""
    return updates


def _route_after_execute(state: NL2SQLState) -> str:
    """Decide whether to finish, retry, or fall back after an execute.

    Per FR-004 / FR-006: retry up to ``max_attempts``; after that, go to
    the last-resort fallback. Success exits to format_result directly.
    """
    result = state.get("result")
    if result is None:
        return "last_resort"
    if not result.error:
        return "success"

    attempt = state.get("attempt", 0)
    max_attempts = state.get("max_attempts", 2)

    if attempt < max_attempts:
        return "retry"
    return "last_resort"


async def fix_sql_node(state: NL2SQLState) -> dict:
    """FR-003: ask the LLM to fix the SQL based on the last error."""
    runtime = _get_runtime_optional()
    llm = _resolve_runtime_dep(state, "llm", runtime["llm"] if runtime else None)
    generated_sql = state["generated_sql"]
    last_error = state.get("last_error", "")
    tables_context = state["tables_context"]
    attempt = state.get("attempt", 0) + 1

    logger.warning(
        "Sandbox query failed (attempt %d): %s | SQL: %s",
        attempt,
        last_error,
        generated_sql[:200],
    )

    retry_messages = [
        LLMMessage(
            role="system",
            content=load_prompt("sql_fixer"),
        ),
        LLMMessage(
            role="user",
            content=(
                f"SQL:\n{generated_sql}\n\nError: {last_error}\n\nTables:\n{tables_context[:2000]}"
            ),
        ),
    ]
    llm_response = await llm.chat(
        messages=retry_messages,
        temperature=0.0,
        max_tokens=1024,
    )

    fixed_sql = _extract_sql(llm_response.content)
    logger.debug("NL2SQL retry generated: %s", fixed_sql[:200])

    return {
        "generated_sql": fixed_sql,
        "attempt": attempt,
    }


async def last_resort_node(state: NL2SQLState) -> dict:
    """FR-006: last-resort fallback — ``SELECT * LIMIT 10`` on the first table.

    Per FR-007, if this also fails (no tables, unsafe name, execution
    error) we still flow through ``format_result`` which will emit a
    clean error ``DataResult``.
    """
    runtime = _get_runtime_optional()
    sandbox = _resolve_runtime_dep(state, "sandbox", runtime["sandbox"] if runtime else None)
    tables = state.get("tables") or []

    if not tables:
        logger.warning("NL2SQL last_resort: no tables available")
        return {"used_fallback": True}

    from app.infrastructure.adapters.sandbox.table_validation import safe_table_query

    fallback_table = tables[0].table_name
    fallback_sql = safe_table_query(fallback_table, 'SELECT * FROM "{}" LIMIT 10')
    if fallback_sql is None:
        logger.warning("NL2SQL last_resort: invalid table name %s", fallback_table)
        return {"used_fallback": True}

    logger.info("NL2SQL last_resort: trying simple query on %s", fallback_table)
    try:
        result = await sandbox.execute_readonly(fallback_sql, timeout_seconds=5)
    except Exception as exc:  # pragma: no cover — defensive
        logger.warning("NL2SQL last_resort raised: %s", exc, exc_info=True)
        return {"used_fallback": True}

    updates: dict[str, Any] = {
        "result": result,
        "used_fallback": True,
    }
    if not result.error:
        updates["generated_sql"] = fallback_sql
        updates["last_error"] = ""
    return updates


async def format_result_node(state: NL2SQLState) -> dict:
    """Format the SQL execution result into a single ``DataResult``.

    Implements FR-012 (exactly one DataResult), FR-013 (required metadata
    on success), FR-014 (``used_fallback`` flag), and FR-015 (APP_ENV-
    based ``generated_sql`` redaction).
    """
    result = state.get("result")
    nl_query = state["nl_query"]
    generated_sql = state.get("generated_sql", "")
    table_descriptions = state.get("table_descriptions", []) or []

    if result is None:
        return {
            "data_results": [
                DataResult(
                    source="sandbox:nl2sql",
                    portal_name="Cache Local (NL2SQL)",
                    portal_url="",
                    dataset_title=f"Consulta SQL fallida: {nl_query[:100]}",
                    format="json",
                    records=[],
                    metadata={
                        "error": "NL2SQL pipeline produced no result",
                        "fetched_at": datetime.now(UTC).isoformat(),
                    },
                )
            ]
        }

    if result.error:
        return {
            "data_results": [
                DataResult(
                    source="sandbox:nl2sql",
                    portal_name="Cache Local (NL2SQL)",
                    portal_url="",
                    dataset_title=f"Consulta SQL fallida: {nl_query[:100]}",
                    format="json",
                    records=[],
                    metadata={
                        "error": result.error,
                        "fetched_at": datetime.now(UTC).isoformat(),
                    },
                )
            ]
        }

    metadata: dict[str, Any] = {
        "total_records": result.row_count,
        "truncated": result.truncated,
        "columns": result.columns,
        "fetched_at": datetime.now(UTC).isoformat(),
        "table_descriptions": table_descriptions,
    }
    # SEC-03: never leak the raw SQL to production responses — it enables
    # schema enumeration through error replay. Keep it in dev/local for
    # debugging.
    if os.getenv("APP_ENV", "local") != "prod":
        metadata["generated_sql"] = generated_sql
    if state.get("used_fallback"):
        metadata["used_fallback"] = True

    return {
        "data_results": [
            DataResult(
                source="sandbox:nl2sql",
                portal_name="Cache Local (NL2SQL)",
                portal_url="",
                dataset_title=f"Consulta SQL: {nl_query[:100]}",
                format="json",
                records=result.rows[:200],
                metadata=metadata,
            )
        ]
    }


def _route_after_format(state: NL2SQLState) -> str:
    """Decide between INDEC fallback, save-success, or END after formatting.

    - INDEC fallback (FR-008) runs when the sandbox returned zero rows AND
      the query matches ``INDEC_PATTERN``.
    - save-success (FR-016) runs when the query was a real success with
      rows, and the result did NOT come from the last-resort fallback
      (FR-018: do not pollute the few-shot cache with ``SELECT *`` rows).
    - Everything else ends the subgraph immediately.
    """
    result = state.get("result")
    if result is None or result.error:
        return "end"
    if not result.rows and state.get("indec_pattern_match"):
        return "indec_fallback"
    if result.rows and not state.get("used_fallback"):
        return "save_success"
    return "end"


async def indec_fallback_node(state: NL2SQLState) -> dict:
    """FR-008: fall back to the live INDEC API when the sandbox is empty."""
    nl_query = state["nl_query"]
    logger.info("NL2SQL: sandbox empty for INDEC query, trying live fallback")

    # Lazy import avoids a cycle (connectors/sandbox.py imports this module).
    from app.application.pipeline.connectors.sandbox import indec_live_fallback

    fallback_results = await indec_live_fallback(nl_query)
    if fallback_results:
        return {"data_results": fallback_results}

    # Keep the existing (empty) format_result output if the live API had
    # nothing either — the previous format_result call already populated
    # data_results with the empty shell.
    return {}


async def save_success_node(state: NL2SQLState) -> dict:
    """FR-016/FR-017/FR-018: persist the successful query as a future few-shot.

    This node is a pure side-effect spawner — it never mutates the output
    data_results. The work happens in a background task whose lifecycle
    is owned by ``_background_tasks.spawn_background``. If embedding or
    semantic_cache are missing (e.g. from a unit-test fixture), the
    coroutine raises on first attribute access and the done callback
    logs a WARNING; no special-case guard is needed here (see spec §4
    note on missing dependencies).
    """
    result = state.get("result")
    if result is None or result.error or not result.rows:
        return {}
    if state.get("used_fallback"):
        # FR-018 — don't save last-resort SELECT * as a few-shot example.
        return {}

    # Lazy import to avoid the cycle with connectors/sandbox.py which
    # re-exports history helpers through its module.
    from app.application.pipeline.history import save_successful_query

    runtime = _get_runtime_optional()
    tables = state.get("tables") or []
    nl_query = state["nl_query"]
    generated_sql = state.get("generated_sql", "")

    spawn_background(
        save_successful_query(
            nl_query,
            generated_sql,
            tables[0].table_name if tables else "",
            result.row_count,
            runtime["embedding"] if runtime else state.get("embedding"),
            runtime["semantic_cache"] if runtime else state.get("semantic_cache"),
        ),
        name="nl2sql.save_success",
    )
    return {}


# ---------------------------------------------------------------------------
# Subgraph builder + compile-once cache
# ---------------------------------------------------------------------------


def build_nl2sql_subgraph():
    """Build and compile the NL2SQL subgraph.

    Prefer ``get_compiled_nl2sql_subgraph()`` in production — it caches
    the compiled instance at module level. Unit tests can call this
    function directly to get a fresh compile per test.
    """
    builder = StateGraph(NL2SQLState)

    builder.add_node("generate_sql", generate_sql_node)
    builder.add_node("execute_sql", execute_sql_node)
    builder.add_node("fix_sql", fix_sql_node)
    builder.add_node("last_resort", last_resort_node)
    builder.add_node("indec_fallback", indec_fallback_node)
    builder.add_node("save_success", save_success_node)
    builder.add_node("format_result", format_result_node)

    builder.add_edge(START, "generate_sql")
    builder.add_edge("generate_sql", "execute_sql")

    builder.add_conditional_edges(
        "execute_sql",
        _route_after_execute,
        {
            "retry": "fix_sql",
            "success": "format_result",
            "last_resort": "last_resort",
        },
    )
    builder.add_edge("fix_sql", "execute_sql")
    builder.add_edge("last_resort", "format_result")

    builder.add_conditional_edges(
        "format_result",
        _route_after_format,
        {
            "indec_fallback": "indec_fallback",
            "save_success": "save_success",
            "end": END,
        },
    )
    builder.add_edge("indec_fallback", END)
    builder.add_edge("save_success", END)

    return builder.compile()


# Module-level compiled subgraph (compile once, reuse).
_compiled_subgraph: Any | None = None
_compile_lock = asyncio.Lock()


async def get_compiled_nl2sql_subgraph():
    """Return the compiled NL2SQL subgraph, compiling it lazily on first use.

    Thread/async-safe via an ``asyncio.Lock`` with a double-check pattern.
    The compiled graph is stateless with respect to per-request data, so
    a single instance can be shared across all requests — every
    invocation ships its own state dict through ``ainvoke()``.
    """
    global _compiled_subgraph  # noqa: PLW0603
    if _compiled_subgraph is not None:
        return _compiled_subgraph
    async with _compile_lock:
        if _compiled_subgraph is None:
            _compiled_subgraph = build_nl2sql_subgraph()
    return _compiled_subgraph
