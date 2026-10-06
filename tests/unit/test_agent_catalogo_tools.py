"""Las herramientas de tablas del agente frente a los casos de la auditoría del 04-oct.

- QW7 / ítem 9: un filtro sin coincidencias en `calcular` devolvía
  `[{'valor': 0}]` o `None` como dato citable. Ahora no hay resultado citable:
  hay un aviso con los valores que sí existen (como el legacy, que los
  buscaba con `discover_values_node`).
- QW3: una columna de texto con números ambiguos ("27.830") no se suma; una
  argentina se suma bien.
- ok.1: `describir_tabla` no se cae si falla la consulta del período.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.application.answers.engine import EngineRequest
from app.application.answers.tools.base import ToolContext, ToolInputError
from app.application.answers.tools.catalogo import Calcular, DescribirTabla, ObtenerDatos
from app.domain.ports.sandbox.sql_sandbox import (
    CachedTableInfo,
    ColumnValueStats,
    SandboxResult,
    TableSource,
    TableValueStats,
)

T = "raw.cache_presupuesto_credito_2026"


class Sandbox:
    """Responde según un texto que contenga el SQL; registra SQL y parámetros."""

    def __init__(
        self,
        types: list[tuple[str, str]],
        responses: list[tuple[str, list[dict[str, Any]] | str]],
        *,
        stats: TableValueStats | None = None,
        row_count: int = 4794,
    ) -> None:
        self.types = types
        self.responses = responses
        self.stats = stats
        self.row_count = row_count
        self.calls: list[tuple[str, Any]] = []

    async def find_tables(self, **kw: Any) -> list[CachedTableInfo]:
        return [CachedTableInfo(T, "ds-1", self.row_count, [])]

    async def describe_marts(self, names: list[str]) -> dict[str, Any]:
        return {}

    async def get_table_sources(self, names: list[str]) -> dict[str, TableSource]:
        return {T.split(".")[1]: TableSource("Presupuesto 2026", "presupuesto", "https://x")}

    async def get_column_types(self, names: list[str]) -> dict[str, list[tuple[str, str]]]:
        return {T: self.types}

    async def get_value_stats(self, table: str, columns: list[str]) -> TableValueStats | None:
        return self.stats

    async def execute_readonly(
        self, sql: str, timeout_seconds: int = 10, *, params: Any = None
    ) -> SandboxResult:
        self.calls.append((sql, params))
        for key, rows in self.responses:
            if key in sql:
                if isinstance(rows, str):
                    kind, _, msg = (
                        rows.partition(":")
                        if rows.startswith("blocked:")
                        else (
                            "timeout",
                            "",
                            rows,
                        )
                    )
                    return SandboxResult([], [], 0, False, error=msg, error_kind=kind)
                return SandboxResult(list(rows[0]) if rows else [], rows, len(rows), False)
        return SandboxResult([], [], 0, False)

    def sql_with(self, text: str) -> str:
        return next(sql for sql, _ in self.calls if text in sql)


def _ctx(sandbox: Sandbox) -> ToolContext:
    deps = MagicMock()
    deps.sandbox = sandbox
    return ToolContext(deps, EngineRequest("q", "u"))


TYPES = [
    ("funcion_desc", "text"),
    ("jurisdiccion_desc", "text"),
    ("credito_devengado", "double precision"),
    ("IMPORTE", "text"),
    ("ejercicio_presupuestario", "bigint"),
]


async def test_calcular_sin_coincidencias_no_da_un_valor_citable() -> None:
    sandbox = Sandbox(
        TYPES,
        [
            ("GROUP BY 1", [{"valor": "Educación y Cultura", "filas": 493}]),
            ("AS valor", [{"valor": 0, "__filas": 0}]),
        ],
    )
    out = await Calcular().run(
        {
            "tabla": T,
            "operacion": "conteo",
            "filtros": [{"columna": "funcion_desc", "operador": "=", "valor": "Educación"}],
        },
        _ctx(sandbox),
    )
    payload = json.loads(out.content)
    assert out.results == []  # nada que citar
    assert payload["resultado"] is None and payload["filas_usadas"] == 0
    assert (
        "Ninguna fila" in payload["aviso"]
        and "«Educación y Cultura» (493 filas)" in (payload["aviso"])
    )
    assert payload["sugerencias"]["funcion_desc"] == [
        {"valor": "Educación y Cultura", "filas": 493}
    ]
    assert out.summary and out.summary.startswith("Ninguna fila")


async def test_calcular_agrupado_sin_filas_tampoco() -> None:
    sandbox = Sandbox(TYPES, [("GROUP BY 1", []), ("AS valor", [])])
    out = await Calcular().run(
        {
            "tabla": T,
            "operacion": "suma",
            "columna": "credito_devengado",
            "agrupar_por": ["jurisdiccion_desc"],
            "filtros": [{"columna": "funcion_desc", "operador": "contiene", "valor": "xyz"}],
        },
        _ctx(sandbox),
    )
    assert out.results == []
    assert json.loads(out.content)["resultado"] is None


async def test_calcular_no_suma_una_columna_de_numeros_ambiguos() -> None:
    sandbox = Sandbox(TYPES, [("AS v FROM", [{"v": "27.830"}, {"v": "7.000"}, {"v": "15.000"}])])
    with pytest.raises(ToolInputError, match="mil veces"):
        await Calcular().run({"tabla": T, "operacion": "suma", "columna": "IMPORTE"}, _ctx(sandbox))
    assert not any("AS valor" in sql for sql, _ in sandbox.calls)  # no llegó a calcular


async def test_calcular_suma_una_columna_argentina_y_dice_sobre_cuantas_filas() -> None:
    # Hacen falta cinco valores que sólo puedan ser argentinos (H009: con uno
    # solo, «1.218.600», se decidía la columna entera). Valores reales de la
    # Pauta CABA.
    muestra = ["27.830", "1.218.600", "1.344.663", "1.446.434", "1.456.840", "1.271.613"]
    sandbox = Sandbox(
        TYPES,
        [
            ("AS v FROM", [{"v": v} for v in muestra]),
            ("AS valor", [{"valor": 242635557, "__filas": 1312, "__filas_con_valor": 1300}]),
        ],
    )
    out = await Calcular().run(
        {"tabla": T, "operacion": "suma", "columna": "IMPORTE"}, _ctx(sandbox)
    )
    sql = sandbox.sql_with("AS valor")
    x = 'btrim("IMPORTE"::text, chr(32) || chr(9) || chr(10) || chr(13) || chr(160))'
    assert f"replace({x}, '.', '')::numeric" in sql
    assert "__filas_ambiguas" not in sql  # formato decidido: no quedan ambiguos sin leer
    [result] = out.results
    assert result.records == [{"valor": 242635557}]  # sin las columnas de control
    assert result.metadata["filas_usadas"] == 1312
    payload = json.loads(out.content)
    assert payload["filas_usadas"] == 1312
    assert payload["filas"] == [{"valor": 242635557, "filas_usadas": 1312}]
    assert any("1300 de 1312" in n for n in payload["notas"])


async def test_calcular_sin_numeros_reconocibles_no_da_valor() -> None:
    sandbox = Sandbox(
        TYPES,
        [
            ("AS v FROM", [{"v": "s/d"}]),
            ("AS valor", [{"valor": None, "__filas": 5, "__filas_con_valor": 0}]),
        ],
    )
    out = await Calcular().run(
        {"tabla": T, "operacion": "suma", "columna": "IMPORTE"}, _ctx(sandbox)
    )
    assert out.results == []
    assert "número reconocible" in json.loads(out.content)["aviso"]


async def test_calcular_separa_los_ambiguos_de_las_filas_sin_numero() -> None:
    """H041: con el formato sin decidir, un «12.500» queda afuera del cálculo,
    pero es un número. El aviso decía que esas filas "no tienen un número
    reconocible". Se cuentan en una consulta aparte (revisión del PR #148: en
    el SELECT principal costaban dos regex por fila en todos los cálculos)."""
    sandbox = Sandbox(
        TYPES,
        [
            ("AS v FROM", [{"v": "500"}, {"v": "1.234,5"}, {"v": "s/d"}]),
            ("AS valor", [{"valor": 1000, "__filas": 100, "__filas_con_valor": 90}]),
            ("AS __filas_ambiguas", [{"__filas_ambiguas": 6}]),
        ],
    )
    out = await Calcular().run(
        {"tabla": T, "operacion": "suma", "columna": "IMPORTE"}, _ctx(sandbox)
    )
    assert "__filas_ambiguas" not in sandbox.sql_with("AS valor")
    cuenta = sandbox.sql_with("AS __filas_ambiguas")
    assert "GROUP BY" not in cuenta and "FILTER" not in cuenta
    [result] = out.results
    assert result.records == [{"valor": 1000}]  # sin las columnas de control
    [aviso] = [n for n in json.loads(out.content)["notas"] if "90 de 100" in n]
    assert "6 tienen un número que se puede leer de dos formas" in aviso
    assert "4 no tienen un número reconocible en 'IMPORTE'" in aviso


async def test_calcular_presupuesto_apn_las_filas_afuera_eran_todas_ambiguas() -> None:
    """Los números de credito_pagado (presupuesto APN `5f0f`, staging) con el
    formato sin decidir: las 24 filas afuera son «6,555» y parecidos."""
    sandbox = Sandbox(
        TYPES,
        [
            ("AS v FROM", [{"v": "500"}]),
            ("AS valor", [{"valor": 38434.9, "__filas": 25063, "__filas_con_valor": 25039}]),
            ("AS __filas_ambiguas", [{"__filas_ambiguas": 24}]),
        ],
    )
    out = await Calcular().run(
        {"tabla": T, "operacion": "suma", "columna": "IMPORTE"}, _ctx(sandbox)
    )
    [aviso] = [n for n in json.loads(out.content)["notas"] if "25039 de 25063" in n]
    assert "las otras 24 tienen un número que se puede leer de dos formas" in aviso
    assert "no tienen un número reconocible" not in aviso


async def test_calcular_sin_valor_por_ambiguos_no_dice_que_no_hay_numeros() -> None:
    sandbox = Sandbox(
        TYPES,
        [
            ("AS v FROM", [{"v": "500"}]),
            ("AS valor", [{"valor": None, "__filas": 5, "__filas_con_valor": 0}]),
            ("AS __filas_ambiguas", [{"__filas_ambiguas": 5}]),
        ],
    )
    out = await Calcular().run(
        {"tabla": T, "operacion": "suma", "columna": "IMPORTE"}, _ctx(sandbox)
    )
    assert out.results == []
    aviso = json.loads(out.content)["aviso"]
    assert "5 tienen un número que se puede leer de dos formas" in aviso
    assert "no tienen un número reconocible" not in aviso


async def test_calcular_con_todas_las_filas_con_valor_no_cuenta_ambiguos() -> None:
    """El caso común (texto con enteros, todas las filas leídas) no paga la
    segunda consulta."""
    sandbox = Sandbox(
        TYPES,
        [
            ("AS v FROM", [{"v": "500"}, {"v": "17"}]),
            ("AS valor", [{"valor": 517, "__filas": 2, "__filas_con_valor": 2}]),
            ("AS __filas_ambiguas", [{"__filas_ambiguas": 0}]),
        ],
    )
    out = await Calcular().run(
        {"tabla": T, "operacion": "suma", "columna": "IMPORTE"}, _ctx(sandbox)
    )
    assert [r.records for r in out.results] == [[{"valor": 517}]]
    assert not any("__filas_ambiguas" in sql for sql, _ in sandbox.calls)


async def test_calcular_si_no_se_pueden_contar_los_ambiguos_da_el_resultado_igual() -> None:
    """Si la cuenta aparte pasa el timeout (tabla grande), el cálculo ya está
    hecho: se informa, y el aviso no afirma que las filas afuera no tengan
    número, porque no se sabe."""
    sandbox = Sandbox(
        TYPES,
        [
            ("AS v FROM", [{"v": "500"}]),
            ("AS valor", [{"valor": 1000, "__filas": 100, "__filas_con_valor": 90}]),
            ("AS __filas_ambiguas", "canceling statement due to statement timeout"),
        ],
    )
    out = await Calcular().run(
        {"tabla": T, "operacion": "suma", "columna": "IMPORTE"}, _ctx(sandbox)
    )
    [result] = out.results
    assert result.records == [{"valor": 1000}]
    [aviso] = [n for n in json.loads(out.content)["notas"] if "90 de 100" in n]
    assert "las otras 10 no tienen un número reconocible en 'IMPORTE' o tienen" in aviso
    assert "se puede leer de dos formas" in aviso


async def test_calcular_avisa_si_hay_mas_grupos_que_el_limite() -> None:
    rows = [
        {"jurisdiccion_desc": f"J{i}", "valor": 10 - i, "__filas": 1, "__filas_con_valor": 1}
        for i in range(3)
    ]
    sandbox = Sandbox(TYPES, [("AS valor", rows)])
    out = await Calcular().run(
        {
            "tabla": T,
            "operacion": "suma",
            "columna": "credito_devengado",
            "agrupar_por": ["jurisdiccion_desc"],
            "limite": 2,
        },
        _ctx(sandbox),
    )
    payload = json.loads(out.content)
    assert len(payload["filas"]) == 2
    assert any("más de 2 grupos" in n for n in payload["notas"])
    assert sandbox.sql_with("AS valor").endswith("LIMIT 3")


async def test_calcular_mayor_que_con_decimales_no_se_rompe() -> None:
    sandbox = Sandbox(TYPES, [("AS valor", [{"valor": 16, "__filas": 16}])])
    await Calcular().run(
        {
            "tabla": T,
            "operacion": "conteo",
            "filtros": [{"columna": "credito_devengado", "operador": ">", "valor": "1000000.5"}],
        },
        _ctx(sandbox),
    )
    sql, params = next((s, p) for s, p in sandbox.calls if "AS valor" in s)
    assert '"credito_devengado" > :p0' in sql
    assert str(params["p0"]) == "1000000.5"


async def test_obtener_datos_en_tabla_grande_filtra_por_el_valor_real() -> None:
    stats = TableValueStats(
        estimated_rows=6_000_000,
        columns={
            "funcion_desc": ColumnValueStats(
                "funcion_desc",
                most_common_vals=["Educación y Cultura", "Salud"],
                most_common_freqs=[0.1, 0.05],
            )
        },
    )
    sandbox = Sandbox(
        TYPES, [("LIMIT 100", [{"funcion_desc": "Educación y Cultura"}])], stats=stats
    )
    out = await ObtenerDatos().run(
        {
            "tabla": T,
            "filtros": [
                {"columna": "funcion_desc", "operador": "=", "valor": "EDUCACION Y CULTURA"}
            ],
        },
        _ctx(sandbox),
    )
    sql, params = next((s, p) for s, p in sandbox.calls if "LIMIT 100" in s)
    # Igualdad exacta con el valor real y con lo pedido: plegar 6 M de filas
    # pasa el timeout.
    assert '"funcion_desc"::text = ANY(:p0)' in sql
    assert 'lower(translate(btrim("funcion_desc"' not in sql
    assert params == {"p0": ["Educación y Cultura", "EDUCACION Y CULTURA"]}
    payload = json.loads(out.content)
    assert (
        "«EDUCACION Y CULTURA» tal cual y como «Educación y Cultura»"
        in (payload["filtros_aplicados"][0])
    )


async def test_obtener_datos_con_fechas_irreconocibles_es_un_error_explicito() -> None:
    sandbox = Sandbox(
        [("periodo", "text"), ("valor", "text")],
        [
            ("AS reconocidas", [{"desde": None, "hasta": None, "reconocidas": 0, "con_valor": 40}]),
            ("LIMIT 3", [{"v": "1er trimestre"}]),
        ],
    )
    with pytest.raises(ToolInputError, match="no tiene fechas en un formato") as exc:
        await ObtenerDatos().run({"tabla": T, "desde": "2020"}, _ctx(sandbox))
    assert "1er trimestre" in str(exc.value)


async def test_obtener_datos_sin_filas_en_el_periodo_dice_el_rango() -> None:
    sandbox = Sandbox(
        [("indice_tiempo", "text"), ("valor", "text")],
        [
            (
                "AS reconocidas",
                [{"desde": "2003-01", "hasta": "2026-06", "reconocidas": 40, "con_valor": 40}],
            ),
        ],
    )
    out = await ObtenerDatos().run({"tabla": T, "desde": "2030"}, _ctx(sandbox))
    payload = json.loads(out.content)
    assert out.results == []
    assert "va de 2003-01 a 2026-06" in payload["aviso"]


async def test_describir_tabla_no_se_cae_si_falla_el_periodo() -> None:
    sandbox = Sandbox(
        [("PUBLICACION_FECHA", "text"), ("TITULO", "text")],
        [("AS reconocidas", "Query timed out after 10 seconds."), ("LIMIT 5", [{"TITULO": "x"}])],
    )
    out = await DescribirTabla().run({"tabla": T}, _ctx(sandbox))
    payload = json.loads(out.content)
    assert payload["columna_fecha"] == "PUBLICACION_FECHA"
    assert payload["desde"] is None and "período" in payload["aviso_fecha"]
    assert payload["muestra"] == [{"TITULO": "x"}]


async def test_describir_tabla_avisa_si_reconoce_menos_del_95_por_ciento() -> None:
    """Revisión independiente del 05-oct, H003: en la Pauta publicitaria de CABA
    (m/d) se reconocían 936 de 3.655 fechas y sólo se avisaba con cero."""
    sandbox = Sandbox(
        [("FECHA", "text"), ("MONTO", "text")],
        [
            (
                "AS reconocidas",
                [
                    {
                        "desde": "2021-01-04",
                        "hasta": "2021-12-11",
                        "reconocidas": 936,
                        "con_valor": 3655,
                    }
                ],
            ),
            ("LIMIT 5", [{"FECHA": "5/31/2021"}]),
        ],
    )
    out = await DescribirTabla().run({"tabla": T}, _ctx(sandbox))
    payload = json.loads(out.content)
    assert payload["desde"] == "2021-01-04"  # el período se informa igual
    assert "936 de 3655" in payload["aviso_fecha"] and "«FECHA»" in payload["aviso_fecha"]


class _SandboxDe(Sandbox):
    """El mismo doble, con otra tabla."""

    def __init__(self, tabla: str, *args: Any, **kw: Any) -> None:
        super().__init__(*args, **kw)
        self.tabla = tabla

    async def find_tables(self, **kw: Any) -> list[CachedTableInfo]:
        return [CachedTableInfo(self.tabla, "ds-2", self.row_count, [])]

    async def get_column_types(self, names: list[str]) -> dict[str, list[tuple[str, str]]]:
        return {self.tabla: self.types}


async def test_describir_tabla_de_nacimientos_no_avisa_que_su_fecha_no_es_la_del_dato() -> None:
    """Revisión del PR #154 (H044): en caba__nacimientos `hijo_fecha_nacimiento`
    es la fecha del dato, pero el aviso decía que no."""
    tabla = "raw.caba__nacimientos__1c5b3921__v2"
    sandbox = _SandboxDe(
        tabla,
        [("hijo_fecha_nacimiento", "text"), ("hijo_genero", "text")],
        [
            (
                "AS reconocidas",
                [{"desde": "2015-01-01", "hasta": "2024-12-31", "reconocidas": 9, "con_valor": 9}],
            ),
        ],
    )
    payload = json.loads((await DescribirTabla().run({"tabla": tabla}, _ctx(sandbox))).content)
    assert payload["columna_fecha"] == "hijo_fecha_nacimiento"
    assert "aviso_fecha" not in payload


# ── revisión del PR #133 ────────────────────────────────────


async def test_calcular_en_tabla_grande_no_pierde_lo_pedido_fuera_de_pg_stats() -> None:
    """Censo de hogares en staging (1,4 M de filas): `en [Gnral.Pueyrredon, Gnral
    Viamonte]` contaba sólo el primero (110.822 en vez de 113.255), sin aviso."""
    stats = TableValueStats(
        estimated_rows=1_426_810,
        columns={
            "jurisdiccion_desc": ColumnValueStats(
                "jurisdiccion_desc", most_common_vals=["Gnral.Pueyrredon", "La Matanza"]
            )
        },
    )
    sandbox = Sandbox(TYPES, [("AS valor", [{"valor": 113255, "__filas": 113255}])], stats=stats)
    out = await Calcular().run(
        {
            "tabla": T,
            "operacion": "conteo",
            "filtros": [
                {
                    "columna": "jurisdiccion_desc",
                    "operador": "en",
                    "valores": ["Gnral.Pueyrredon", "Gnral Viamonte"],
                }
            ],
        },
        _ctx(sandbox),
    )
    sql, params = next((s, p) for s, p in sandbox.calls if "AS valor" in s)
    assert '"jurisdiccion_desc"::text = ANY(:p0)' in sql
    assert set(params["p0"]) == {"Gnral.Pueyrredon", "Gnral Viamonte"}
    notas = json.loads(out.content)["notas"]
    assert any("«Gnral Viamonte» tal cual" in n for n in notas)


async def test_calcular_con_mas_grupos_que_el_limite_informa_el_total_de_todos() -> None:
    rows = [
        {
            "jurisdiccion_desc": f"J{i}",
            "valor": 10 - i,
            "__filas": 5,
            "__filas_con_valor": 5,
            "__filas_total": 500,
            "__filas_con_valor_total": 480,
        }
        for i in range(3)
    ]
    sandbox = Sandbox(TYPES, [("AS valor", rows)])
    out = await Calcular().run(
        {
            "tabla": T,
            "operacion": "suma",
            "columna": "credito_devengado",
            "agrupar_por": ["jurisdiccion_desc"],
            "limite": 2,
        },
        _ctx(sandbox),
    )
    payload = json.loads(out.content)
    [result] = out.results
    # Antes: 10 (la suma de los dos grupos mostrados) presentado como el total.
    assert payload["filas_usadas"] == 500 and result.metadata["filas_usadas"] == 500
    assert result.metadata["truncada"] is True  # la clave del contrato de metadatos
    assert any("480 de 500" in n for n in payload["notas"])
    assert all("__filas" not in k for r in result.records for k in r)


async def test_calcular_cortado_sin_el_total_no_lo_presenta_como_total() -> None:
    rows = [
        {"jurisdiccion_desc": f"J{i}", "valor": 10 - i, "__filas": 5, "__filas_con_valor": 5}
        for i in range(3)
    ]
    sandbox = Sandbox(TYPES, [("AS valor", rows)])
    out = await Calcular().run(
        {
            "tabla": T,
            "operacion": "suma",
            "columna": "credito_devengado",
            "agrupar_por": ["jurisdiccion_desc"],
            "limite": 2,
        },
        _ctx(sandbox),
    )
    payload = json.loads(out.content)
    [result] = out.results
    assert "filas_usadas" not in payload and "filas_usadas" not in result.metadata
    assert payload["filas_usadas_en_grupos_mostrados"] == 10


async def test_calcular_avisa_si_las_fechas_se_leen_solo_en_su_forma_dominante() -> None:
    """Tabla grande con 92 % d/m/aaaa y 8 % ISO: con `desde`, las ISO quedaban
    afuera en silencio (`filas_usadas` cuenta las que pasaron el WHERE)."""
    muestra = [f"{d}/3/2025" for d in range(1, 24)] + ["2025-03-01", "2025-03-02"]
    stats = TableValueStats(
        estimated_rows=2_000_000,
        columns={"fecha": ColumnValueStats("fecha", histogram_bounds=muestra)},
    )
    sandbox = Sandbox(
        [("fecha", "text"), ("monto", "double precision")],
        [("AS valor", [{"valor": 7, "__filas": 7, "__filas_con_valor": 7}])],
        stats=stats,
    )
    out = await Calcular().run(
        {"tabla": T, "operacion": "suma", "columna": "monto", "desde": "2025-01"},
        _ctx(sandbox),
    )
    assert "(CASE WHEN NULLIF(btrim(" in sandbox.sql_with("AS valor")  # la versión con guarda
    notas = json.loads(out.content)["notas"]
    assert any("forma dominante (d/m/aaaa)" in n for n in notas)


async def test_describir_tabla_bloqueada_no_inventa_un_periodo_de_pg_stats() -> None:
    stats = TableValueStats(
        estimated_rows=10,
        columns={
            "PUBLICACION_FECHA": ColumnValueStats(
                "PUBLICACION_FECHA", histogram_bounds=["2008-03-03", "2026-03-05"]
            )
        },
    )
    sandbox = Sandbox(
        [("PUBLICACION_FECHA", "text"), ("TITULO", "text")],
        [("AS reconocidas", "blocked:La tabla tiene un problema de calidad sin resolver")],
        stats=stats,
    )
    out = await DescribirTabla().run({"tabla": T}, _ctx(sandbox))
    payload = json.loads(out.content)
    assert payload["desde"] is None and payload["hasta"] is None
    assert "problema de calidad" in payload["aviso_fecha"]
