"""NL2SQL subgraph: encapsulates SQL generation, execution, and retry loop.

NOTE: This subgraph is NOT yet integrated into the main pipeline graph.
The sandbox connector (connectors/sandbox.py) still handles NL2SQL directly.
This subgraph will replace the inline logic in Phase 4 (fan-out refactor).

Extracts the SQL gen/exec/retry logic from ``connectors/sandbox.py`` into a
proper LangGraph subgraph with explicit nodes and conditional edges.

Table discovery remains in ``sandbox.py`` — only the SQL generation,
execution, and self-correction loop lives here.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from app.domain.entities.connectors.data_result import DataResult
from app.domain.ports.llm.llm_provider import LLMMessage
from app.prompts import load_prompt

logger = logging.getLogger(__name__)


class NL2SQLState(TypedDict, total=False):
    """State flowing through the NL2SQL subgraph."""

    nl_query: str
    tables_context: str
    few_shot_block: str
    nl2sql_prompt: str
    generated_sql: str
    result: Any  # SandboxResult
    attempt: int
    max_attempts: int
    last_error: str
    # Dependencies passed in (not serialisable — only used within a single invocation)
    llm: Any  # ILLMProvider
    sandbox: Any  # ISQLSandbox
    # Output
    data_results: list  # list[DataResult]


def _strip_markdown_fences(sql: str) -> str:
    """Strip ```sql ... ``` markdown fences from LLM output."""
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


async def generate_sql_node(state: NL2SQLState) -> dict:
    """Generate SQL from natural language using the LLM."""
    llm = state["llm"]
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

    return {
        "nl2sql_prompt": nl2sql_prompt,
        "generated_sql": generated_sql,
        "attempt": 0,
    }


async def execute_sql_node(state: NL2SQLState) -> dict:
    """Execute the generated SQL against the sandbox."""
    sandbox = state["sandbox"]
    generated_sql = state["generated_sql"]

    result = await sandbox.execute_readonly(generated_sql)

    updates: dict[str, Any] = {"result": result}
    if result.error:
        updates["last_error"] = result.error
    return updates


def should_retry(state: NL2SQLState) -> str:
    """Decide whether to retry, succeed, or give up after SQL execution."""
    result = state.get("result")
    if result is None or not result.error:
        return "success"

    attempt = state.get("attempt", 0)
    max_attempts = state.get("max_attempts", 2)

    if attempt < max_attempts:
        return "retry"
    return "give_up"


async def fix_sql_node(state: NL2SQLState) -> dict:
    """Fix the SQL query using LLM based on the error message."""
    llm = state["llm"]
    generated_sql = state["generated_sql"]
    last_error = state.get("last_error", "")
    tables_context = state["tables_context"]
    attempt = state.get("attempt", 0) + 1

    logger.warning("Sandbox query failed (attempt %d): %s", attempt, last_error)

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

    return {
        "generated_sql": fixed_sql,
        "attempt": attempt,
    }


async def format_result_node(state: NL2SQLState) -> dict:
    """Format the SQL execution result into DataResult objects."""
    result = state.get("result")
    nl_query = state["nl_query"]
    generated_sql = state.get("generated_sql", "")

    if result is None:
        return {"data_results": []}

    if result.error:
        # All retries exhausted — return error result
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

    return {
        "data_results": [
            DataResult(
                source="sandbox:nl2sql",
                portal_name="Cache Local (NL2SQL)",
                portal_url="",
                dataset_title=f"Consulta SQL: {nl_query[:100]}",
                format="json",
                records=result.rows[:200],
                metadata={
                    "total_records": result.row_count,
                    "generated_sql": generated_sql,
                    "truncated": result.truncated,
                    "columns": result.columns,
                    "fetched_at": datetime.now(UTC).isoformat(),
                },
            )
        ]
    }


def build_nl2sql_subgraph():
    """Build and compile the NL2SQL subgraph.

    Flow:
        START -> generate_sql -> execute_sql
                                  |-> success -> format_result -> END
                                  |-> retry   -> fix_sql -> execute_sql
                                  |-> give_up -> format_result -> END
    """
    builder = StateGraph(NL2SQLState)

    builder.add_node("generate_sql", generate_sql_node)
    builder.add_node("execute_sql", execute_sql_node)
    builder.add_node("fix_sql", fix_sql_node)
    builder.add_node("format_result", format_result_node)

    builder.add_edge(START, "generate_sql")
    builder.add_edge("generate_sql", "execute_sql")
    builder.add_conditional_edges(
        "execute_sql",
        should_retry,
        {
            "retry": "fix_sql",
            "success": "format_result",
            "give_up": "format_result",
        },
    )
    builder.add_edge("fix_sql", "execute_sql")
    builder.add_edge("format_result", END)

    return builder.compile()
