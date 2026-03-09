from __future__ import annotations

from typing import Any

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Request
from pydantic import BaseModel, Field

from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage
from app.domain.ports.sandbox.sql_sandbox import ISQLSandbox
from app.prompts import load_prompt
from app.setup.app_factory import limiter

router = APIRouter(prefix="/sandbox", tags=["sandbox"])


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    sql: str = Field(..., min_length=1, max_length=5000)


class QueryResponse(BaseModel):
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    truncated: bool
    error: str | None = None


class TableInfo(BaseModel):
    table_name: str
    dataset_id: str
    row_count: int | None
    columns: list[str]


class AskRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=2000)


class AskResponse(BaseModel):
    sql: str
    columns: list[str]
    rows: list[dict[str, Any]]
    row_count: int
    truncated: bool
    error: str | None = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/query", response_model=QueryResponse)
@limiter.limit("10/minute")  # type: ignore[untyped-decorator]
@inject  # type: ignore[untyped-decorator]
async def execute_query(
    request: Request,
    body: QueryRequest,
    sandbox: FromDishka[ISQLSandbox],
) -> QueryResponse:
    """Execute a raw SQL SELECT query against cached dataset tables."""
    result = await sandbox.execute_readonly(body.sql)
    return QueryResponse(
        columns=result.columns,
        rows=result.rows,
        row_count=result.row_count,
        truncated=result.truncated,
        error=result.error,
    )


@router.get("/tables", response_model=list[TableInfo])
@inject  # type: ignore[untyped-decorator]
async def list_tables(
    sandbox: FromDishka[ISQLSandbox],
) -> list[TableInfo]:
    """List all cached dataset tables with their columns and row counts."""
    tables = await sandbox.list_cached_tables()
    return [
        TableInfo(
            table_name=t.table_name,
            dataset_id=t.dataset_id,
            row_count=t.row_count,
            columns=t.columns,
        )
        for t in tables
    ]


@router.post("/ask", response_model=AskResponse)
@limiter.limit("10/minute")  # type: ignore[untyped-decorator]
@inject  # type: ignore[untyped-decorator]
async def ask_natural_language(
    request: Request,
    body: AskRequest,
    sandbox: FromDishka[ISQLSandbox],
    llm: FromDishka[ILLMProvider],
) -> AskResponse:
    """
    Natural language to SQL: takes a question in plain language, uses the LLM
    to generate a SELECT query, executes it against the cached tables, and
    returns the results together with the generated SQL.

    Includes column types in the schema context and a self-correction loop
    that retries up to 2 times if the generated SQL fails.
    """
    # 1. Gather table metadata for context
    tables = await sandbox.list_cached_tables()
    if not tables:
        return AskResponse(
            sql="",
            columns=[],
            rows=[],
            row_count=0,
            truncated=False,
            error="No cached tables available. Import datasets first.",
        )

    # 2. Get column types for enriched context
    table_names = [t.table_name for t in tables]
    column_types = await sandbox.get_column_types(table_names)

    tables_context_parts = []
    for t in tables:
        type_info = column_types.get(t.table_name, [])
        if type_info:
            cols = ", ".join(f"{name} ({dtype})" for name, dtype in type_info)
        elif t.columns:
            cols = ", ".join(t.columns)
        else:
            cols = "(no column info)"
        tables_context_parts.append(
            f"Table: {t.table_name}  (rows: {t.row_count or '?'})\n  Columns: {cols}"
        )
    tables_context = "\n\n".join(tables_context_parts)

    # 3. Ask the LLM to generate SQL
    system_prompt = load_prompt("nl2sql", tables_context=tables_context)
    messages = [
        LLMMessage(role="system", content=system_prompt),
        LLMMessage(role="user", content=body.question),
    ]
    llm_response = await llm.chat(
        messages=messages,
        temperature=0.0,
        max_tokens=1024,
    )
    generated_sql = _strip_sql_fences(llm_response.content.strip())

    # 4. Execute with self-correction loop (max 2 retries on error)
    max_retries = 2
    result = await sandbox.execute_readonly(generated_sql)

    for _attempt in range(max_retries):
        if not result.error:
            break
        # Ask the LLM to fix the SQL with error context
        messages.extend([
            LLMMessage(role="assistant", content=generated_sql),
            LLMMessage(
                role="user",
                content=(
                    f"Esa query SQL devolvió un error: {result.error}\n"
                    "Corregí la query y devolvé solo el SQL corregido."
                ),
            ),
        ])
        llm_response = await llm.chat(
            messages=messages, temperature=0.0, max_tokens=1024,
        )
        generated_sql = _strip_sql_fences(llm_response.content.strip())
        result = await sandbox.execute_readonly(generated_sql)

    return AskResponse(
        sql=generated_sql,
        columns=result.columns,
        rows=result.rows,
        row_count=result.row_count,
        truncated=result.truncated,
        error=result.error,
    )


def _strip_sql_fences(sql: str) -> str:
    """Strip markdown code fences if the LLM wraps the SQL."""
    if sql.startswith("```"):
        lines = sql.split("\n")
        lines = [ln for ln in lines if not ln.strip().startswith("```")]
        sql = "\n".join(lines).strip()
    return sql
