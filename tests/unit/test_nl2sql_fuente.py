"""La fuente de una respuesta NL2SQL es el dataset publicado, con su link.

Caso real (staging, 29-sep): la respuesta del BCRA citaba "Consulta SQL:
Mostrame las tasas…" en "Cache Local (NL2SQL)", sin URL. La tabla era
`raw.datos_gob_ar__principales_tasas_de_interes__6335b6d1__v1`, del dataset
"Principales tasas de interés" de datos.gob.ar.

Estos tests corren el nodo como en producción: el sandbox llega por
`nl2sql_runtime(...)` y NO por el state (connectors/sandbox.py). Buscarlo sólo
en el state era un no-op silencioso para la fuente y un RuntimeError en la
medición de filas excluidas.
"""

from __future__ import annotations

from typing import Any

from app.application.pipeline.subgraphs.nl2sql import format_result_node, nl2sql_runtime
from app.domain.ports.sandbox.sql_sandbox import SandboxResult, TableSource

_TABLE = "raw.datos_gob_ar__principales_tasas_de_interes__6335b6d1__v1"
_CSV = (
    "https://infra.datos.gob.ar/catalog/sspm/dataset/89/distribution/89.2/download/"
    "principales-tasas-interes-diarias.csv"
)


class FakeSandbox:
    def __init__(self, sources: dict[str, TableSource] | Exception | None = None) -> None:
        self.sources = sources if sources is not None else {}
        self.asked: list[list[str]] = []
        self.probes: list[str] = []

    async def get_table_sources(self, table_names: list[str]) -> dict[str, TableSource]:
        self.asked.append(table_names)
        if isinstance(self.sources, Exception):
            raise self.sources
        return self.sources

    async def execute_readonly(self, sql: str, timeout_seconds: int = 10) -> SandboxResult:
        self.probes.append(sql)
        return SandboxResult(
            columns=["total", "excluded"],
            rows=[{"total": 100, "excluded": 7}],
            row_count=1,
            truncated=False,
        )


def _state(sql: str) -> dict[str, Any]:
    return {
        "nl_query": "Mostrame las tasas de interes del BCRA en el ultimo año",
        "generated_sql": sql,
        "result": SandboxResult(
            columns=["indice_tiempo", "tasas_interes_call"],
            rows=[
                {"indice_tiempo": "2025-12-01", "tasas_interes_call": 23.33},
                {"indice_tiempo": "2025-12-02", "tasas_interes_call": 23.11},
            ],
            row_count=2,
            truncated=False,
        ),
    }


async def _run(sandbox: FakeSandbox, sql: str) -> Any:
    with nl2sql_runtime(llm=None, sandbox=sandbox, embedding=None, semantic_cache=None):
        out = await format_result_node(_state(sql))  # type: ignore[arg-type]
    return out["data_results"][0]


_SQL = (
    f"SELECT indice_tiempo, tasas_interes_call FROM {_TABLE} ORDER BY indice_tiempo DESC LIMIT 200"
)


class TestFuenteReal:
    async def test_cites_the_published_dataset_with_its_link(self) -> None:
        sandbox = FakeSandbox(
            {
                "datos_gob_ar__principales_tasas_de_interes__6335b6d1__v1": TableSource(
                    title="Principales tasas de interés", portal="datos_gob_ar", url=_CSV
                )
            }
        )
        result = await _run(sandbox, _SQL)
        assert result.dataset_title == "Principales tasas de interés"
        assert result.portal_url == _CSV
        assert result.portal_name == "infra.datos.gob.ar"
        assert sandbox.asked == [[_TABLE]]
        assert result.metadata["served_table"] == _TABLE

    async def test_unknown_table_keeps_the_query_label(self) -> None:
        result = await _run(FakeSandbox({}), _SQL)
        assert result.dataset_title.startswith("Consulta SQL: Mostrame")
        assert result.portal_name == "Cache Local (NL2SQL)"
        assert result.portal_url == ""

    async def test_lookup_failure_never_breaks_the_answer(self) -> None:
        result = await _run(FakeSandbox(ConnectionError("db down")), _SQL)
        assert result.dataset_title.startswith("Consulta SQL:")
        assert len(result.records) == 2

    async def test_marts_are_not_looked_up(self) -> None:
        sandbox = FakeSandbox({})
        await _run(sandbox, "SELECT anio, total FROM mart.series_economicas LIMIT 10")
        assert sandbox.asked == []


class TestCoberturaConElSandboxDelRuntime:
    async def test_lossy_filter_is_measured_instead_of_raising(self) -> None:
        # Antes: `_resolve_runtime_dep(state, "sandbox", None)` → RuntimeError,
        # porque en producción el state no trae el sandbox.
        sql = (
            "SELECT jurisdiccion_desc, SUM(CAST(credito_devengado AS NUMERIC)) AS total "
            "FROM raw.presupuesto WHERE credito_devengado ~ '^\\-?\\d+\\.?\\d*$' "
            "GROUP BY jurisdiccion_desc ORDER BY total DESC LIMIT 10"
        )
        sandbox = FakeSandbox({})
        result = await _run(sandbox, sql)
        coverage = result.metadata["coverage_warning"]
        assert coverage["measured"] is True
        assert coverage["excluded_rows"] == 7
        assert sandbox.probes, "la medición tiene que haber corrido contra el sandbox"
