# Plan: SQL Sandbox (As-Built)

**Related spec**: [./spec.md](./spec.md)
**Last synced with code**: 2026-04-10

---

## 1. Hexagonal Mapping

| Layer | Component | File |
|---|---|---|
| Domain Port | `ISQLSandbox` | `domain/ports/sandbox/sql_sandbox.py` |
| Infrastructure Adapter | `PgSandboxAdapter` | `infrastructure/adapters/sandbox/` |
| Infrastructure Helper | `table_validation.py` | `infrastructure/adapters/sandbox/table_validation.py` |
| Presentation | `sandbox_router.py` | `presentation/http/controllers/sandbox/sandbox_router.py` |

## 2. Port

```python
class ISQLSandbox(ABC):
    async def execute_readonly(
        self,
        sql: str,
        timeout_seconds: int = 10,
    ) -> SandboxResult: ...

    async def list_cached_tables(self) -> list[CachedTableInfo]: ...

    async def get_column_types(
        self,
        table_names: list[str],
    ) -> dict[str, list[tuple[str, str]]]: ...
```

## 3. SandboxResult

```python
@dataclass
class SandboxResult:
    columns: list[str]
    rows: list[list]
    row_count: int
    truncated: bool
    error: str | None
```

## 4. Validation (3 layers)

### Layer 1: Regex
Reject: INSERT, UPDATE, DELETE, DROP, ALTER, CREATE, TRUNCATE, GRANT, REVOKE, COPY, VACUUM, ANALYZE (and variants).

### Layer 2: Allowlist
Only SELECT or WITH (CTE). All table refs must be `cache_*` in schema `public`.

### Layer 3: sqlglot AST
Parse SQL with `sqlglot.parse_one(sql, dialect="postgres")`. Walk AST:
- Reject if any `DML`, `DDL`, `Command` nodes exist
- Validate every `Table` node has allowed name
- Check CTEs recursively

## 5. Execution Flow

```python
async def execute_readonly(sql, timeout_seconds):
    error = _validate_sql(sql)
    if error:
        return SandboxResult(columns=[], rows=[], row_count=0, truncated=False, error=error)

    def _run():
        with engine.connect() as conn:
            conn.execute(text(f"SET LOCAL statement_timeout = {timeout_seconds * 1000}"))
            conn.execute(text("SET LOCAL default_transaction_read_only = on"))
            result = conn.execute(text(sql))
            rows = result.fetchmany(MAX_ROWS + 1)
            truncated = len(rows) > MAX_ROWS
            return SandboxResult(
                columns=list(result.keys()),
                rows=[list(r) for r in rows[:MAX_ROWS]],
                row_count=min(len(rows), MAX_ROWS),
                truncated=truncated,
                error=None,
            )

    return await asyncio.get_event_loop().run_in_executor(self._executor, _run)
```

`self._executor = ThreadPoolExecutor(max_workers=2)`.

## 6. Endpoints

| Method | Path | Rate Limit | Behavior |
|---|---|---|---|
| POST | `/api/v1/sandbox/query` | 10/min | Execute raw SQL |
| GET | `/api/v1/sandbox/tables` | — | List cache_* tables |
| POST | `/api/v1/sandbox/ask` | 10/min | NL2SQL with self-correction |

## 7. NL2SQL Flow

```
POST /sandbox/ask {"question": "..."}
    ↓
is_suspicious(question)? → 400 if yes
    ↓
Load table metadata → build context
    ↓
LLM generate SQL (prompt "nl2sql.md")
    ↓
execute_readonly(sql)
    ├→ success → return {sql, rows, ...}
    └→ error → LLM fix_sql(sql, error) → execute
              ├→ success
              └→ error → LLM fix again (attempt 2)
                        ├→ success
                        └→ give up → return {error: "..."}
```

## 8. Source Files

- `domain/ports/sandbox/sql_sandbox.py`
- `infrastructure/adapters/sandbox/pg_sandbox_adapter.py`
- `infrastructure/adapters/sandbox/table_validation.py`
- `presentation/http/controllers/sandbox/sandbox_router.py`
- `application/pipeline/subgraphs/nl2sql.py` — not integrated
- `application/pipeline/connectors/sandbox.py` — pipeline step
- `prompts/nl2sql.md` — prompt template

## 9. Deviations from Constitution

- Complies with security and hexagonal principles. The ThreadPoolExecutor is an accepted exception (sync sqlalchemy engine).

---

**End of plan.md**
