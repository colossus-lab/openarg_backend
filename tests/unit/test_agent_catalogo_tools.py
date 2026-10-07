"""Las herramientas de tablas del agente frente a los casos de la auditoría del 04-oct.

- QW7 / ítem 9: un filtro sin coincidencias en `calcular` devolvía
  `[{'valor': 0}]` o `None` como dato citable. Ahora no hay resultado citable:
  hay un aviso con los valores que sí existen (como el legacy, que los
  buscaba con `discover_values_node`).
- QW3: una columna de texto con números ambiguos ("27.830") no se suma; una
  argentina se suma bien.
- ok.1: `describir_tabla` no se cae si falla la consulta del período.
- Prueba del 06-oct (nueva_16): `describir_tabla` con `row_count` 0 en el
  catálogo, y el aviso geográfico de una tabla de un portal provincial.
- Revisión de #165: un conteo del catálogo que Postgres no confirma, y una
  tabla cortada en el tope del colector.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from app.application.answers.engine import EngineRequest
from app.application.answers.tools.base import ToolContext, ToolInputError
from app.application.answers.tools.catalogo import Calcular, DescribirTabla, ObtenerDatos
from app.application.answers.verification import verify_figures
from app.domain.ports.sandbox.sql_sandbox import (
    CachedTableInfo,
    ColumnValueStats,
    SandboxResult,
    TableProfile,
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


async def test_calcular_conteo_con_columna_vuelve_al_modelo_sin_rotular_mal() -> None:
    """H109 (revisión del 05-oct): antes salía «conteo de credito_devengado»
    con el total de filas. Ahora el modelo recibe el porqué y reintenta."""
    sandbox = Sandbox(TYPES, [("AS valor", [{"valor": 10, "__filas": 10}])])
    with pytest.raises(ToolInputError, match="conteo"):
        await Calcular().run(
            {"tabla": T, "operacion": "conteo", "columna": "credito_devengado"}, _ctx(sandbox)
        )
    assert sandbox.calls == []  # no llegó a consultar


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
            # En la tabla chica, la columna entera no tiene valores ingleses.
            ("AS del_otro", [{"del_otro": 0}]),
            ("AS valor", [{"valor": 242635557, "__filas": 1312, "__filas_con_valor": 1300}]),
        ],
    )
    out = await Calcular().run(
        {"tabla": T, "operacion": "suma", "columna": "IMPORTE"}, _ctx(sandbox)
    )
    assert "IMPORTE" in sandbox.sql_with("AS del_otro")
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


class _SandboxDePortal(_SandboxDe):
    """El mismo doble, con otra tabla y otro portal."""

    def __init__(self, tabla: str, portal: str, *args: Any, **kw: Any) -> None:
        super().__init__(tabla, *args, **kw)
        self.portal = portal

    async def get_table_sources(self, names: list[str]) -> dict[str, TableSource]:
        return {
            self.tabla.split(".")[1]: TableSource("Tarifa Social Eléctrica 2022", self.portal, "")
        }


# La tabla de nueva_16 (prueba de calidad del 06-oct en staging): 110.179
# filas, `raw.cached_datasets.row_count` = 0 y ninguna columna geográfica.
TARIFA = "raw.mendoza__tarifa_social_electrica_2022__adfd4b97__v1"
TARIFA_TIPOS = [
    ("DISTRIBUIDORA", "text"),
    ("SUMINISTRO", "text"),
    ("SITUACION", "text"),
    ("PADRON", "bigint"),
    ("SINTYS", "text"),
]


async def test_describir_tabla_con_cero_filas_en_el_catalogo_da_la_estimacion() -> None:
    """Chequeo del 06-oct: 18.134 de 31.236 tablas listas de staging tienen
    `row_count` 0 en el catálogo aunque tengan filas. `describir_tabla` le
    decía al modelo `"filas": 0` y al usuario «(0 filas)»."""
    sandbox = _SandboxDePortal(
        TARIFA,
        "mendoza",
        TARIFA_TIPOS,
        [],
        stats=TableValueStats(estimated_rows=110_179),
        row_count=0,
    )
    out = await DescribirTabla().run({"tabla": TARIFA}, _ctx(sandbox))
    payload = json.loads(out.content)
    assert payload["filas"] is None  # no es un conteo: no va como tal
    assert payload["filas_estimadas"] == 110_179
    assert "estimación" in payload["aviso_filas"] and "calcular" in payload["aviso_filas"]
    assert out.summary == "Revisó «Tarifa Social Eléctrica 2022» (unas 110.179 filas)"


async def test_describir_tabla_sin_conteo_ni_estimacion_lo_dice() -> None:
    sandbox = _SandboxDePortal(TARIFA, "mendoza", TARIFA_TIPOS, [], stats=None, row_count=0)
    out = await DescribirTabla().run({"tabla": TARIFA}, _ctx(sandbox))
    payload = json.loads(out.content)
    assert payload["filas"] is None and "filas_estimadas" not in payload
    assert "no tiene la cantidad de filas" in payload["aviso_filas"]
    assert out.summary == "Revisó «Tarifa Social Eléctrica 2022»"


async def test_describir_tabla_con_el_conteo_del_catalogo_confirmado_no_avisa() -> None:
    """Antes este test fijaba creerle al catálogo aunque Postgres dijera otra
    cosa (catálogo 4.794, estimación 1). La revisión de #165 mostró que el
    conteo del catálogo también está mal cuando no es 0: ahora vale sólo si
    Postgres dice lo mismo (ver el test siguiente)."""
    sandbox = _SandboxDePortal(
        TARIFA,
        "mendoza",
        TARIFA_TIPOS,
        [],
        stats=TableValueStats(estimated_rows=4794),
        row_count=4794,
    )
    out = await DescribirTabla().run({"tabla": TARIFA}, _ctx(sandbox))
    payload = json.loads(out.content)
    assert payload["filas"] == 4794
    assert "filas_estimadas" not in payload and "aviso_filas" not in payload
    assert out.summary == "Revisó «Tarifa Social Eléctrica 2022» (4.794 filas)"


async def test_describir_tabla_con_un_conteo_viejo_en_el_catalogo_da_la_estimacion() -> None:
    """Revisión de #165: la Tarifa Social Eléctrica de Mendoza 2019 anuncia
    107.067 filas en `raw.cached_datasets` y tiene 7.065 (`count(*)` y
    `reltuples`, staging). En staging, 1.482 de las 11.491 tablas listas con
    conteo en el catálogo no coinciden con `reltuples`; en una muestra de 45
    tablas en que no coincidían, `reltuples` era el `count(*)` en 44."""
    sandbox = _SandboxDePortal(
        "raw.mendoza__tarifa_social_electrica_2019__613931d3__v1",
        "mendoza",
        TARIFA_TIPOS,
        [],
        stats=TableValueStats(estimated_rows=7065),
        row_count=107_067,
    )
    out = await DescribirTabla().run(
        {"tabla": "raw.mendoza__tarifa_social_electrica_2019__613931d3__v1"}, _ctx(sandbox)
    )
    payload = json.loads(out.content)
    assert payload["filas"] is None
    assert payload["filas_estimadas"] == 7065
    assert "estimación" in payload["aviso_filas"] and "calcular" in payload["aviso_filas"]
    assert "107" not in out.content and "107" not in (out.summary or "")
    assert out.summary == "Revisó «Tarifa Social Eléctrica 2022» (unas 7.065 filas)"


async def test_describir_tabla_sin_estimacion_usa_el_conteo_del_catalogo() -> None:
    """Sin estadísticas (tabla bloqueada, o nunca analizada) no hay contra qué
    comparar: queda el conteo del catálogo, como antes."""
    sandbox = _SandboxDePortal(TARIFA, "mendoza", TARIFA_TIPOS, [], stats=None, row_count=4794)
    out = await DescribirTabla().run({"tabla": TARIFA}, _ctx(sandbox))
    payload = json.loads(out.content)
    assert payload["filas"] == 4794 and "aviso_filas" not in payload
    assert out.summary == "Revisó «Tarifa Social Eléctrica 2022» (4.794 filas)"


class _SandboxConPerfil(_SandboxDePortal):
    """Con lo que registró el colector de la versión viva (`table_profiles`)."""

    def __init__(self, *args: Any, perfil: TableProfile, **kw: Any) -> None:
        super().__init__(*args, **kw)
        self.perfil = perfil

    async def table_profiles(self, names: list[str]) -> dict[str, TableProfile]:
        return {self.perfil.table_name: self.perfil}


DEFUNCIONES = "raw.caba__defunciones__91003a9e__v1"
DEFUNCIONES_TIPOS = [("anio", "bigint"), ("causa", "text"), ("sexo", "text")]


@pytest.mark.parametrize(
    ("row_count", "estimada", "perfil", "tope"),
    [
        # Revisión de #165: catálogo en 0 y `reltuples` en el tope (211 tablas
        # en staging, entre ellas caba__defunciones y caba__elecciones_2015).
        (0, 500_000, None, "500.000"),
        # Catálogo y Postgres de acuerdo en el tope: antes no avisaba nada.
        (500_000, 500_000, None, "500.000"),
        # Marcada cortada y con `reltuples` estimado debajo del tope (42 en
        # staging): el tope sale de las filas de la versión viva.
        (0, 499_986, TableProfile("caba__defunciones__91003a9e__v1", 500_000, True), "500.000"),
        # Un tope de otra época del colector.
        (2_500_000, 2_500_000, None, "2.500.000"),
    ],
)
async def test_describir_tabla_cortada_en_el_tope_no_pide_contar_el_total(
    row_count: int, estimada: int, perfil: TableProfile | None, tope: str
) -> None:
    """Revisión de #165: a una tabla que el colector cortó en el tope le
    decía «Para un total, contalo con calcular (operacion=conteo)». Ese
    conteo es el tope, no el total de la fuente."""
    kw: dict[str, Any] = {"stats": TableValueStats(estimated_rows=estimada), "row_count": row_count}
    if perfil is None:
        sandbox: _SandboxDePortal = _SandboxDePortal(
            DEFUNCIONES, "caba", DEFUNCIONES_TIPOS, [], **kw
        )
    else:
        sandbox = _SandboxConPerfil(DEFUNCIONES, "caba", DEFUNCIONES_TIPOS, [], perfil=perfil, **kw)
    out = await DescribirTabla().run({"tabla": DEFUNCIONES}, _ctx(sandbox))
    payload = json.loads(out.content)
    # Lo guardado es justo el tope, aunque `reltuples` lo estime un poco debajo.
    assert payload["filas"] == int(tope.replace(".", "")) and "filas_estimadas" not in payload
    aviso = payload["aviso_filas"]
    assert f"cortada en {tope} filas" in aviso
    assert "no el total de la fuente" in aviso
    assert "Para un total" not in aviso
    assert out.summary == f"Revisó «Tarifa Social Eléctrica 2022» (al menos {tope} filas)"


async def test_un_conteo_viejo_del_catalogo_en_el_tope_no_marca_cortada_la_tabla() -> None:
    """Si Postgres dice otra cosa, el 500.000 del catálogo es de otra versión."""
    sandbox = _SandboxDePortal(
        DEFUNCIONES,
        "caba",
        DEFUNCIONES_TIPOS,
        [],
        stats=TableValueStats(estimated_rows=7065),
        row_count=500_000,
    )
    out = await DescribirTabla().run({"tabla": DEFUNCIONES}, _ctx(sandbox))
    payload = json.loads(out.content)
    assert payload["filas"] is None and payload["filas_estimadas"] == 7065
    assert "cortada" not in payload["aviso_filas"]
    assert out.summary == "Revisó «Tarifa Social Eléctrica 2022» (unas 7.065 filas)"


async def test_describir_tabla_marcada_cortada_sin_tope_conocido_lo_dice_sin_numero() -> None:
    perfil = TableProfile("caba__defunciones__91003a9e__v1", None, True)
    sandbox = _SandboxConPerfil(
        DEFUNCIONES,
        "caba",
        DEFUNCIONES_TIPOS,
        [],
        perfil=perfil,
        stats=TableValueStats(estimated_rows=491_667),
        row_count=0,
    )
    out = await DescribirTabla().run({"tabla": DEFUNCIONES}, _ctx(sandbox))
    payload = json.loads(out.content)
    assert payload["aviso_filas"].startswith("La tabla está cortada:")
    assert "Para un total" not in payload["aviso_filas"]
    assert out.summary == "Revisó «Tarifa Social Eléctrica 2022» (al menos 491.667 filas)"


async def test_describir_tabla_de_un_portal_provincial_no_dice_nacional() -> None:
    """Chequeo del 06-oct: a por lo menos 444 tablas de portales provinciales
    y municipales sin columnas geográficas les decía que el dato era
    "normalmente el total nacional" y que diera "la cifra nacional"."""
    sandbox = _SandboxDePortal(TARIFA, "mendoza", TARIFA_TIPOS, [])
    payload = json.loads((await DescribirTabla().run({"tabla": TARIFA}, _ctx(sandbox))).content)
    assert payload["columnas_geograficas"] == []
    aviso = payload["aviso_geografico"]
    assert "la provincia de Mendoza" in aviso
    assert "nacional" not in aviso.lower()


@pytest.mark.parametrize(
    ("portal", "nivel"),
    [
        ("caba", "la Ciudad de Buenos Aires"),
        ("buenos_aires_prov", "la provincia de Buenos Aires"),
        ("cordoba_estadistica", "la provincia de Córdoba"),
        ("entre_rios", "la provincia de Entre Ríos"),
        ("rosario_dkan", "la ciudad de Rosario"),
        ("ciudad_mendoza", "la ciudad de Mendoza"),
        # Un portal local que todavía no está en la lista: se nombra, sin
        # decir que es nacional.
        ("portal_nuevo", "lo que cubre el portal «portal_nuevo»"),
    ],
)
async def test_el_aviso_geografico_nombra_el_nivel_del_portal(portal: str, nivel: str) -> None:
    sandbox = _SandboxDePortal(TARIFA, portal, TARIFA_TIPOS, [])
    payload = json.loads((await DescribirTabla().run({"tabla": TARIFA}, _ctx(sandbox))).content)
    assert nivel in payload["aviso_geografico"]
    assert "nacional" not in payload["aviso_geografico"].lower()


@pytest.mark.parametrize("portal", ["datos_gob_ar", "indec", "energia", "datos.gob.ar", ""])
async def test_en_un_portal_nacional_el_aviso_sigue_siendo_el_nacional(portal: str) -> None:
    """La batería del 02-oct (Pinamar): con el aviso nacional, Sonnet y Haiku
    dan la cifra nacional que la tabla sí tiene. ``datos.gob.ar`` es lo que
    pone ``resolve`` si la tabla no tiene fuente."""
    sandbox = _SandboxDePortal(TARIFA, portal, TARIFA_TIPOS, [])
    payload = json.loads((await DescribirTabla().run({"tabla": TARIFA}, _ctx(sandbox))).content)
    assert "DÁ IGUAL la cifra nacional" in payload["aviso_geografico"]


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


# ── prueba de calidad del 07-oct (ola 4): nueva_16 ─────────────────────────
#
# El agente agrupó la Tarifa Social de Mendoza por PADRON (el mes de alta en
# el padrón: 33 valores, de 201702 a 202207), se quedó con PADRON = 202207 y
# presentó esas 99.558 filas como el padrón de 2022. Son 110.179: las otras
# 10.621 también están «EN PADRON». `calcular` devolvía el conteo filtrado
# sin decir de cuántas filas de la tabla era una parte.

# Lo que devolvió `calcular` en la corrida (q25_ola4.json, nueva_16), igual a
# lo que da hoy sobre la tabla de staging.
_SITUACION_202207 = [
    ("EN PADRON TS-JUBILADOS Y PENSIONADOS", 35870),
    ("EN PADRON TS-TRANSITORIOS", 24712),
    ("EN PADRON TS-PROGRAMAS SOCIALES NO MONETARIOS", 11011),
    ("EN PADRON TS-PROGRAMAS SOCIALES", 10580),
    ("EN PADRON TS-PNC", 7954),
    ("EN PADRON TS-EMPLEO DEPENDIENTE", 7109),
    ("EN PADRON TS-SERVICIO DOMESTICO", 1202),
    ("EN PADRON TS-REGISTRO CASOS ESPECIALES", 463),
    ("EN PADRON TS-ELECTRODEPENDIENTES", 262),
    ("EN PADRON TS-DESEMPLEO", 207),
    ("EN PADRON TS-CASOS TRANSITORIOS", 117),
    ("EN PADRON TS-VETERANOS DE GUERRA", 52),
    ("EN PADRON TS-REGISTRO CASOS ESPECIALES-1569", 15),
    ("EN PADRON TS-MEDIDORES COMUNITARIOS", 3),
    ("EN PADRON TS-POSEE UN INGRESO SUPERIOR A 2 SMVM", 1),
]
_FILAS_202207 = [
    {"SITUACION": s, "valor": n, "__filas": n, "__filas_total": 99_558}
    for s, n in _SITUACION_202207
]
_PIDE_202207 = {
    "tabla": TARIFA,
    "operacion": "conteo",
    "agrupar_por": ["SITUACION"],
    "filtros": [{"columna": "PADRON", "operador": "=", "valor": "202207"}],
}


def _tarifa(respuestas: list[tuple[str, Any]], estimadas: int | None = 110_179) -> Sandbox:
    stats = TableValueStats(estimated_rows=estimadas) if estimadas else None
    return _SandboxDePortal(TARIFA, "mendoza", TARIFA_TIPOS, respuestas, stats=stats, row_count=0)


async def test_calcular_conteo_filtrado_dice_de_cuantas_filas_de_la_tabla_es_parte() -> None:
    sandbox = _tarifa([("AS filas_tabla", [{"filas_tabla": 110_179}]), ("AS valor", _FILAS_202207)])
    out = await Calcular().run(_PIDE_202207, _ctx(sandbox))
    payload = json.loads(out.content)
    assert payload["filas_usadas"] == 99_558
    assert payload["filas_tabla"] == 110_179
    aviso = payload["aviso_parte"]
    assert aviso.startswith(
        "Los filtros dejaron afuera 10621 filas: la tabla entera tiene 110179, y 99558 es sólo "
        "la parte que queda."
    )
    # Verificación de la ola 5: el aviso no le deja leer PADRON = 202207 como
    # una foto mensual. La tabla no tiene columna de fecha: es una sola foto,
    # y su total es el conteo sin filtros.
    assert "si el filtro elige un período, una foto" not in aviso
    assert "La tabla no apila períodos (no tiene columna de fecha): es una sola foto" in aviso
    assert "su total es el conteo sin filtros, 110179" in aviso
    assert "Filtrar por «PADRON» no elige otro período ni otra foto" in aviso
    # Sin columna de fecha no hay período que leer.
    assert not any("AS reconocidas" in sql for sql, _ in sandbox.calls)
    # El conteo de toda la tabla, sin el filtro.
    cuenta = sandbox.sql_with("AS filas_tabla")
    assert "WHERE" not in cuenta and "GROUP BY" not in cuenta
    assert TARIFA.split(".")[1] in cuenta
    # Y no se cita como evidencia: la única es el conteo filtrado.
    [result] = out.results
    assert result.metadata["filas_usadas"] == 99_558


async def test_calcular_conteo_sin_filtros_no_vuelve_a_contar_la_tabla() -> None:
    """Sin filtros, el conteo ya es de toda la tabla: no se paga otra consulta."""
    filas = [
        {"PADRON": 202207, "valor": 99_558, "__filas": 99_558, "__filas_total": 110_179},
        {"PADRON": 201702, "valor": 1983, "__filas": 1983, "__filas_total": 110_179},
    ]
    sandbox = _tarifa([("AS filas_tabla", [{"filas_tabla": 110_179}]), ("AS valor", filas)])
    out = await Calcular().run(
        {"tabla": TARIFA, "operacion": "conteo", "agrupar_por": ["PADRON"]}, _ctx(sandbox)
    )
    payload = json.loads(out.content)
    assert payload["filas_usadas"] == 110_179
    assert "filas_tabla" not in payload and "aviso_parte" not in payload
    assert not any("filas_tabla" in sql for sql, _ in sandbox.calls)
    assert len(out.results) == 1


async def test_calcular_suma_filtrada_no_cuenta_la_tabla() -> None:
    """Sólo el conteo: el total de una suma no es la cantidad de filas."""
    sandbox = Sandbox(
        TYPES,
        [
            ("AS filas_tabla", [{"filas_tabla": 4794}]),
            ("AS valor", [{"valor": 1000, "__filas": 10, "__filas_con_valor": 10}]),
        ],
    )
    out = await Calcular().run(
        {
            "tabla": T,
            "operacion": "suma",
            "columna": "credito_devengado",
            "filtros": [{"columna": "jurisdiccion_desc", "operador": "=", "valor": "Salud"}],
        },
        _ctx(sandbox),
    )
    assert "aviso_parte" not in json.loads(out.content)
    assert not any("filas_tabla" in sql for sql, _ in sandbox.calls)


async def test_calcular_conteo_con_un_filtro_que_no_deja_nada_afuera_no_avisa() -> None:
    filas = [{**f, "__filas_total": 110_179} for f in _FILAS_202207]
    sandbox = _tarifa([("AS filas_tabla", [{"filas_tabla": 110_179}]), ("AS valor", filas)])
    out = await Calcular().run(_PIDE_202207, _ctx(sandbox))
    payload = json.loads(out.content)
    assert "filas_tabla" not in payload and "aviso_parte" not in payload
    assert len(out.results) == 1


async def test_calcular_conteo_filtrado_en_tabla_grande_no_cuenta_la_tabla_entera() -> None:
    """En una tabla de millones de filas el `count(*)` pasaría el tope: va la
    estimación de la base, dicha como tal."""
    sandbox = _tarifa(
        [("AS filas_tabla", [{"filas_tabla": 2_400_000}]), ("AS valor", _FILAS_202207)],
        estimadas=2_400_000,
    )
    out = await Calcular().run(_PIDE_202207, _ctx(sandbox))
    payload = json.loads(out.content)
    assert not any("filas_tabla" in sql for sql, _ in sandbox.calls)
    assert "filas_tabla" not in payload
    aviso = payload["aviso_parte"]
    assert "unas 2400000" in aviso and "estimación" in aviso
    assert "el total es" not in aviso
    # La estimación no se da como el total de la foto: que lo cuente.
    assert "su total es el conteo sin filtros (contalo con calcular, sin filtros)" in aviso
    assert len(out.results) == 1


async def test_calcular_conteo_filtrado_si_falla_el_conteo_de_la_tabla_da_el_resultado() -> None:
    sandbox = _tarifa(
        [
            ("AS filas_tabla", "canceling statement due to statement timeout"),
            ("AS valor", _FILAS_202207),
        ]
    )
    out = await Calcular().run(_PIDE_202207, _ctx(sandbox))
    payload = json.loads(out.content)
    assert payload["filas_usadas"] == 99_558
    assert "filas_tabla" not in payload
    assert "unas 110179" in payload["aviso_parte"]
    [result] = out.results
    assert result.metadata["filas_usadas"] == 99_558


async def test_calcular_conteo_filtrado_si_el_conteo_de_la_tabla_explota_no_se_cae() -> None:
    class _Explota(_SandboxDePortal):
        async def execute_readonly(
            self, sql: str, timeout_seconds: int = 10, *, params: Any = None
        ) -> SandboxResult:
            if "AS filas_tabla" in sql:
                raise RuntimeError("se cortó la conexión")
            return await super().execute_readonly(sql, timeout_seconds, params=params)

    sandbox = _Explota(
        TARIFA,
        "mendoza",
        TARIFA_TIPOS,
        [("AS valor", _FILAS_202207)],
        stats=TableValueStats(estimated_rows=110_179),
        row_count=0,
    )
    out = await Calcular().run(_PIDE_202207, _ctx(sandbox))
    payload = json.loads(out.content)
    assert payload["filas_usadas"] == 99_558 and "filas_tabla" not in payload
    assert "unas 110179" in payload["aviso_parte"]


# ── revisión de #171: las filas de la tabla no son «el total» ──────────────
#
# En una tabla que apila períodos, fotos o tipos de fila, contar sin filtros
# no da el total de nada. En staging: los homicidios del SNIC son 20.801
# imputados y 17.325 víctimas (38.126 filas); el rendimiento de
# establecimientos de salud de PBA, 19 años de unos 2.300 establecimientos
# (41.354 filas, 2.406 establecimientos distintos); los transportes
# autorizados de CABA, 13 fotos mensuales del padrón (52.367 filas). La
# primera versión de #171 decía «el total es N» y lo citaba como evidencia:
# el verificador daba por respaldado el total inflado, y en la batería la
# fuente de complex_003 dejaba de ser «sólo conteos chicos».

HOMICIDIOS = "raw.datos_gob_ar__homicidios_dolosos_sistema_de_alert__2ba03073__v1"
HOMICIDIOS_TIPOS = [
    ("id_hecho", "text"),
    ("tipo_persona", "text"),
    ("provincia_nombre", "text"),
    ("anio", "bigint"),
    ("mes", "bigint"),
    ("fecha_hecho", "text"),
]


# El rango de `fecha_hecho` en la tabla del SNIC de staging.
_RANGO_HOMICIDIOS = [
    {"desde": "2014-01-01", "hasta": "2023-12-31", "reconocidas": 38_126, "con_valor": 38_126}
]


def _homicidios(respuestas: list[tuple[str, Any]]) -> Sandbox:
    return _SandboxDePortal(
        HOMICIDIOS,
        "datos_gob_ar",
        HOMICIDIOS_TIPOS,
        [("AS reconocidas", _RANGO_HOMICIDIOS), *respuestas],
        stats=TableValueStats(estimated_rows=38_126),
        row_count=38_126,
    )


async def test_calcular_conteo_de_un_tipo_de_fila_no_da_las_filas_de_la_tabla_como_total() -> None:
    sandbox = _homicidios(
        [
            ("AS filas_tabla", [{"filas_tabla": 38_126}]),
            ("AS valor", [{"valor": 17_325, "__filas": 17_325}]),
        ]
    )
    pide = {
        "tabla": HOMICIDIOS,
        "operacion": "conteo",
        "filtros": [{"columna": "tipo_persona", "operador": "=", "valor": "Víctima"}],
    }
    out = await Calcular().run(pide, _ctx(sandbox))
    payload = json.loads(out.content)
    assert payload["filas_usadas"] == 17_325 and payload["filas_tabla"] == 38_126
    aviso = payload["aviso_parte"]
    assert aviso.startswith(
        "Los filtros dejaron afuera 20801 filas: la tabla entera tiene 38126, y 17325 es sólo "
        "la parte que queda."
    )
    # La tabla apila períodos: lo dice con la columna que los distingue, y no
    # llama total a sus filas (verificación de la ola 5).
    assert (
        "La tabla apila períodos en «fecha_hecho» (de 2014-01-01 a 2023-12-31): sus 38126 "
        "filas juntan todos esos períodos, no son el total de uno solo."
    ) in aviso
    assert "el total es" not in aviso and "su total es" not in aviso
    assert "una sola foto" not in aviso
    # Sin segunda evidencia: el total inflado no queda respaldado.
    [result] = out.results
    assert result.records == [{"valor": 17_325}]
    verificado = verify_figures("El SNIC registra 38.126 homicidios dolosos en total.", out.results)
    assert [c.status for c in verificado.checks] == ["sin_respaldo"]


RENDIMIENTO = "raw.cache_buenos_aires_rendimiento_de_establecimientos__r576cd5036b"
RENDIMIENTO_TIPOS = [("establecimiento_id", "text"), ("anio", "bigint"), ("egresos", "bigint")]
TRANSPORTES = "raw.caba__transportes_autorizados__1a2b3c4d__v1"
TRANSPORTES_TIPOS = [("barrio", "text"), ("periodo", "text"), ("numero_documento", "text")]
DEFUNCIONES_MES = "raw.caba__defunciones__91003a9e__v1"
DEFUNCIONES_MES_TIPOS = [("anio", "bigint"), ("mes", "bigint"), ("causa", "text")]


@pytest.mark.parametrize(
    ("tabla", "tipos", "columna", "valor", "filas", "parte"),
    [
        # Un año de una tabla con una fila por establecimiento y año.
        (RENDIMIENTO, RENDIMIENTO_TIPOS, "anio", "2023", 41_354, 2361),
        # La última foto de un padrón apilado (`periodo` es su columna_fecha).
        (TRANSPORTES, TRANSPORTES_TIPOS, "periodo", "2018 SEPTIEMBRE", 52_367, 4458),
        # El mes de una tabla con año y mes separados.
        (DEFUNCIONES_MES, DEFUNCIONES_MES_TIPOS, "mes", "3", 120_000, 10_000),
    ],
)
async def test_calcular_conteo_con_filtro_de_periodo_no_avisa_ni_cuenta_la_tabla(
    tabla: str,
    tipos: list[tuple[str, str]],
    columna: str,
    valor: str,
    filas: int,
    parte: int,
) -> None:
    """Un filtro sobre la columna de fecha elige un período: contar sin él suma
    todos los períodos, no da un total."""
    sandbox = _SandboxDePortal(
        tabla,
        "x",
        tipos,
        [
            ("AS filas_tabla", [{"filas_tabla": filas}]),
            ("AS valor", [{"valor": parte, "__filas": parte}]),
        ],
        stats=TableValueStats(estimated_rows=filas),
        row_count=filas,
    )
    pide = {
        "tabla": tabla,
        "operacion": "conteo",
        "filtros": [{"columna": columna, "operador": "=", "valor": valor}],
    }
    out = await Calcular().run(pide, _ctx(sandbox))
    payload = json.loads(out.content)
    assert payload["filas_usadas"] == parte
    assert "filas_tabla" not in payload and "aviso_parte" not in payload
    assert not any("filas_tabla" in sql for sql, _ in sandbox.calls)
    assert len(out.results) == 1


# ── verificación de la ola 5 sobre #171 ───────────────────────────────────
#
# Dos hallazgos. El aviso genérico («si el filtro elige un período, una foto…,
# la tabla los suma a todos») le daba al modelo la salida para quedarse con
# PADRON = 202207, que parece justo una foto mensual. Y la forma natural de
# pedir «en 2022» (`columna_fecha=PADRON` con `desde`/`hasta`) seguía dando
# 99.558 sin aviso, porque contaba como un filtro de período. PADRON no es la
# fecha de la tabla: la tabla no tiene columna de fecha.


async def test_calcular_conteo_en_2022_por_padron_con_desde_hasta_avisa_que_es_una_parte() -> None:
    """n16_alt_periodo: antes de esta revisión, 99.558 sin aviso."""
    sandbox = _tarifa(
        [
            ("AS filas_tabla", [{"filas_tabla": 110_179}]),
            ("AS valor", [{"valor": 99_558, "__filas": 99_558}]),
        ]
    )
    pide = {
        "tabla": TARIFA,
        "operacion": "conteo",
        "columna_fecha": "PADRON",
        "desde": "2022-01-01",
        "hasta": "2022-12-31",
    }
    out = await Calcular().run(pide, _ctx(sandbox))
    payload = json.loads(out.content)
    assert payload["filas_usadas"] == 99_558
    assert payload["filas_tabla"] == 110_179
    assert payload["aviso_parte"] == (
        "El período pedido sobre «PADRON» dejó afuera 10621 filas: la tabla entera tiene 110179, "
        "y 99558 es sólo la parte que queda. La tabla no apila períodos (no tiene columna de "
        "fecha): es una sola foto, así que su total es el conteo sin filtros, 110179. Filtrar por "
        "«PADRON» no elige otro período ni otra foto: elige una parte de esa foto."
    )
    [result] = out.results
    assert result.records == [{"valor": 99_558}]


async def test_calcular_conteo_por_padron_como_columna_fecha_y_filtro_tambien_avisa() -> None:
    """`columna_fecha=PADRON` con el filtro `PADRON = 202207`: antes contaba como
    filtro de período y no avisaba."""
    sandbox = _tarifa([("AS filas_tabla", [{"filas_tabla": 110_179}]), ("AS valor", _FILAS_202207)])
    out = await Calcular().run({**_PIDE_202207, "columna_fecha": "PADRON"}, _ctx(sandbox))
    aviso = json.loads(out.content)["aviso_parte"]
    assert "su total es el conteo sin filtros, 110179" in aviso


async def test_calcular_conteo_con_periodo_de_la_tabla_que_los_apila_y_otro_filtro_no_avisa() -> (
    None
):
    """Víctimas de 2023 en el SNIC: el período va sobre la fecha de la tabla, que
    apila años. Contar sin el período no da un total: no avisa ni cuenta la tabla."""
    sandbox = _homicidios(
        [
            ("AS filas_tabla", [{"filas_tabla": 38_126}]),
            ("AS valor", [{"valor": 1_790, "__filas": 1_790}]),
        ]
    )
    pide = {
        "tabla": HOMICIDIOS,
        "operacion": "conteo",
        "desde": "2023",
        "hasta": "2023",
        "filtros": [{"columna": "tipo_persona", "operador": "=", "valor": "Víctima"}],
    }
    out = await Calcular().run(pide, _ctx(sandbox))
    payload = json.loads(out.content)
    assert payload["filas_usadas"] == 1_790
    assert "filas_tabla" not in payload and "aviso_parte" not in payload
    assert not any("AS filas_tabla" in sql for sql, _ in sandbox.calls)


async def test_calcular_conteo_con_otra_columna_de_periodo_en_tabla_que_apila_no_miente() -> None:
    """`anio = 2023` en el SNIC (su fecha es `fecha_hecho`): el aviso dice que la
    tabla apila períodos y no afirma que la parte junte todos los años."""
    sandbox = _homicidios(
        [
            ("AS filas_tabla", [{"filas_tabla": 38_126}]),
            ("AS valor", [{"valor": 3_500, "__filas": 3_500}]),
        ]
    )
    pide = {
        "tabla": HOMICIDIOS,
        "operacion": "conteo",
        "filtros": [{"columna": "anio", "operador": "=", "valor": "2023"}],
    }
    aviso = json.loads((await Calcular().run(pide, _ctx(sandbox))).content)["aviso_parte"]
    assert "apila períodos en «fecha_hecho»" in aviso
    assert "una sola foto" not in aviso and "su total es" not in aviso


PADRON_ANUAL = "raw.x__padron_anual__1a2b3c4d__v1"
PADRON_ANUAL_TIPOS = [("anio", "bigint"), ("categoria", "text")]
ALTAS = "raw.x__beneficiarios__1a2b3c4d__v1"


async def test_calcular_conteo_en_tabla_de_un_solo_periodo_dice_que_es_una_foto() -> None:
    sandbox = _SandboxDePortal(
        PADRON_ANUAL,
        "x",
        PADRON_ANUAL_TIPOS,
        [
            (
                "AS reconocidas",
                [{"desde": "2022", "hasta": "2022", "reconocidas": 9, "con_valor": 9}],
            ),
            ("AS filas_tabla", [{"filas_tabla": 1000}]),
            ("AS valor", [{"valor": 300, "__filas": 300}]),
        ],
        stats=TableValueStats(estimated_rows=1000),
        row_count=1000,
    )
    pide = {
        "tabla": PADRON_ANUAL,
        "operacion": "conteo",
        "filtros": [{"columna": "categoria", "operador": "=", "valor": "A"}],
    }
    aviso = json.loads((await Calcular().run(pide, _ctx(sandbox))).content)["aviso_parte"]
    assert (
        "La tabla no apila períodos (toda la tabla es de 2022 según «anio»): es una sola foto, así "
        "que su total es el conteo sin filtros, 1000."
    ) in aviso


@pytest.mark.parametrize(
    ("desde", "hasta", "avisa"),
    [
        ("2022", "2022", True),  # abarca de 2022-01-01 a 2022-12-31
        ("2021-06", "2023", True),
        ("2022-06", "2022-12", False),  # deja afuera enero a mayo: un período de la tabla
    ],
)
async def test_calcular_conteo_con_un_periodo_que_abarca_toda_la_tabla(
    desde: str, hasta: str, avisa: bool
) -> None:
    sandbox = _SandboxDePortal(
        PADRON_ANUAL,
        "x",
        [("fecha", "text"), ("categoria", "text")],
        [
            (
                "AS reconocidas",
                [{"desde": "2022-01-01", "hasta": "2022-12-31", "reconocidas": 9, "con_valor": 9}],
            ),
            ("AS filas_tabla", [{"filas_tabla": 1000}]),
            ("AS valor", [{"valor": 300, "__filas": 300}]),
        ],
        stats=TableValueStats(estimated_rows=1000),
        row_count=1000,
    )
    pide = {
        "tabla": PADRON_ANUAL,
        "operacion": "conteo",
        "desde": desde,
        "hasta": hasta,
        "filtros": [{"columna": "categoria", "operador": "=", "valor": "A"}],
    }
    payload = json.loads((await Calcular().run(pide, _ctx(sandbox))).content)
    if not avisa:
        assert "aviso_parte" not in payload and "filas_tabla" not in payload
        return
    assert payload["aviso_parte"] == (
        "Los filtros dejaron afuera 700 filas: la tabla entera tiene 1000, y 300 es sólo la parte "
        "que queda. El período pedido abarca toda la tabla (de 2022-01-01 a 2022-12-31 según "
        "«fecha»), así que el total de ese período es el conteo sin filtros, 1000."
    )


@pytest.mark.parametrize(
    ("tabla", "tipos", "pide", "dudosa"),
    [
        # Sin fecha, pero con una columna de mes: puede apilar meses.
        (
            PADRON_ANUAL,
            [("mes", "text"), ("categoria", "text")],
            {"filtros": [{"columna": "categoria", "operador": "=", "valor": "A"}]},
            "«mes» puede distinguirlos",
        ),
        # Su única fecha es de alta: describe a cada fila y puede no separar
        # períodos (un padrón como el de Mendoza, con la fecha reconocida).
        (
            ALTAS,
            [("fecha_alta", "text"), ("corte", "text"), ("categoria", "text")],
            {"desde": "2022", "hasta": "2022"},
            "«fecha_alta» o «corte» pueden distinguirlos",
        ),
    ],
)
async def test_calcular_conteo_sin_saber_si_la_tabla_apila_periodos_lo_dice(
    tabla: str, tipos: list[tuple[str, str]], pide: dict[str, Any], dudosa: str
) -> None:
    sandbox = _SandboxDePortal(
        tabla,
        "x",
        tipos,
        [
            ("AS filas_tabla", [{"filas_tabla": 1000}]),
            ("AS valor", [{"valor": 300, "__filas": 300}]),
        ],
        stats=TableValueStats(estimated_rows=1000),
        row_count=1000,
    )
    out = await Calcular().run({"tabla": tabla, "operacion": "conteo", **pide}, _ctx(sandbox))
    aviso = json.loads(out.content)["aviso_parte"]
    assert f"No sé si la tabla apila períodos: {dudosa}." in aviso
    assert "La tabla no apila períodos" not in aviso
