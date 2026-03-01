from __future__ import annotations

from dishka.integrations.fastapi import FromDishka, inject
from fastapi import APIRouter, Request
from pydantic import BaseModel, Field
from slowapi import Limiter
from slowapi.util import get_remote_address

from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage
from app.domain.ports.sandbox.sql_sandbox import ISQLSandbox

router = APIRouter(prefix="/sandbox", tags=["sandbox"])
limiter = Limiter(key_func=get_remote_address)


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------

class QueryRequest(BaseModel):
    sql: str = Field(..., min_length=1, max_length=5000)


class QueryResponse(BaseModel):
    columns: list[str]
    rows: list[dict]
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
    rows: list[dict]
    row_count: int
    truncated: bool
    error: str | None = None


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/query", response_model=QueryResponse)
@limiter.limit("10/minute")
@inject
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
@inject
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


_NL2SQL_SYSTEM_PROMPT = """\
You are a SQL assistant for Argentine public datasets stored in PostgreSQL.
You MUST generate ONLY a single SELECT query. Never use INSERT, UPDATE, DELETE, \
DROP, or any DDL/DML statement.

Available tables and their columns:

{tables_context}

Rules:
- Only reference the tables and columns listed above.
- Always use double-quoted identifiers for column names that contain spaces or \
special characters.
- Limit results to 1000 rows max (add LIMIT 1000 if appropriate).
- Return ONLY the SQL query, nothing else. No markdown, no explanation.
- The query must be valid PostgreSQL syntax.
"""


@router.post("/ask", response_model=AskResponse)
@limiter.limit("10/minute")
@inject
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

    tables_context_parts = []
    for t in tables:
        cols = ", ".join(t.columns) if t.columns else "(no column info)"
        tables_context_parts.append(
            f"Table: {t.table_name}  (rows: {t.row_count or '?'})\n  Columns: {cols}"
        )
    tables_context = "\n\n".join(tables_context_parts)

    # 2. Ask the LLM to generate SQL
    system_prompt = _NL2SQL_SYSTEM_PROMPT.format(tables_context=tables_context)
    llm_response = await llm.chat(
        messages=[
            LLMMessage(role="system", content=system_prompt),
            LLMMessage(role="user", content=body.question),
        ],
        temperature=0.0,
        max_tokens=1024,
    )

    generated_sql = llm_response.content.strip()
    # Strip markdown code fences if the LLM wraps the SQL
    if generated_sql.startswith("```"):
        lines = generated_sql.split("\n")
        # Remove first and last lines (``` markers)
        lines = [l for l in lines if not l.strip().startswith("```")]
        generated_sql = "\n".join(lines).strip()

    # 3. Execute the generated SQL
    result = await sandbox.execute_readonly(generated_sql)

    return AskResponse(
        sql=generated_sql,
        columns=result.columns,
        rows=result.rows,
        row_count=result.row_count,
        truncated=result.truncated,
        error=result.error,
    )
