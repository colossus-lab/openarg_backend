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


async def generate_sql_node(state: NL2SQLState) -> dict:
    """Generate SQL from natural language using the LLM."""
    llm = state["llm"]
    nl_query = state["nl_query"]
    tables_context = state["tables_context"]
    few_shot_block = state.get("few_shot_block", "")

    nl2sql_prompt = (
        "You are a SQL assistant for Argentine public datasets stored in PostgreSQL.\n"
        "You MUST generate ONLY a single SELECT query. Never use INSERT, UPDATE, DELETE, "
        "DROP, or any DDL/DML statement.\n\n"
        f"Available tables and their columns:\n\n{tables_context}\n\n"
        "Rules:\n"
        "- Only reference the tables and columns listed above.\n"
        "- Always use double-quoted identifiers for column names that contain spaces or special characters.\n"
        "- Limit results to 1000 rows max (add LIMIT 1000 if appropriate).\n"
        "- Return ONLY the SQL query, nothing else. No markdown, no explanation.\n"
        "- The query must be valid PostgreSQL syntax.\n"
        "- NEVER use parameter placeholders like $1, $2, :param, or ?. Always use literal values in the query.\n"
        "- Use ILIKE for case-insensitive text matching.\n"
    )
    if few_shot_block:
        nl2sql_prompt += f"\n{few_shot_block}\n"

    messages = [
        LLMMessage(role="system", content=nl2sql_prompt),
        LLMMessage(role="user", content=nl_query),
    ]

    llm_response = await llm.chat(
        messages=messages,
        temperature=0.0,
        max_tokens=1024,
    )

    generated_sql = _strip_markdown_fences(llm_response.content.strip())

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
            content="Fix this PostgreSQL query. Return ONLY the corrected SQL, no markdown.",
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

    fixed_sql = _strip_markdown_fences(llm_response.content.strip())

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
