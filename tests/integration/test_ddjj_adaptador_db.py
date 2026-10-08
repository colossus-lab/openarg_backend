"""Las consultas del adaptador de DDJJ contra Postgres de verdad.

Lo que se prueba vive en el SQL:

- una declaración por persona y año (la anual si hay varias);
- la exclusión de las inconsistentes y el conteo de las que habrían entrado;
- la mediana y los patrimonios negativos;
- la búsqueda sin tildes y por CUIT, con el detalle de bienes;
- la evolución, con homónimos;
- CABA, que no tiene patrimonio neto.

Las tablas se crean acá con la definición de `ddjj_tasks._DDL` y se borran al
terminar (en CI la base es descartable).
"""

from __future__ import annotations

import os
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import text

from app.infrastructure.adapters.connectors import ddjj_adapter as da
from app.infrastructure.celery.tasks import _db
from app.infrastructure.celery.tasks import ddjj_tasks as dt

M = 1_000_000


def _engine_or_skip():
    if not os.getenv("DATABASE_URL"):
        pytest.skip("DATABASE_URL not set — este test necesita una DB real")
    try:
        engine = _db.get_sync_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        return engine
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")


def _decl(
    dj_id: int,
    nombre: str,
    *,
    anio: int = 2024,
    tipo: str = "Anual",
    patrimonio: float,
    cuit: str | None = None,
    cargo: str = "Diputado Nacional",
    organismo: str = "HONORABLE CAMARA DE DIPUTADOS DE LA NACION",
    poder: str = "legislativo",
    ingresos: float = 10 * M,
    inconsistente: bool = False,
    ingresos_inconsistentes: bool = False,
    fuente: str = da.FUENTE_OA,
) -> dict[str, Any]:
    oa = fuente == da.FUENTE_OA
    return {
        "fuente": fuente,
        "dj_id": dj_id,
        "jurisdiccion": "nacional" if oa else "caba",
        "poder": poder,
        "cuit": cuit,
        "nombre": nombre,
        "anio": anio,
        "tipo": tipo if oa else None,
        "rectificativa": 0,
        "organismo": organismo if oa else None,
        "cargo": cargo,
        "bienes": Decimal(patrimonio if oa else patrimonio),
        "deudas": Decimal(0) if oa else None,
        "patrimonio": Decimal(patrimonio) if oa else None,
        "bienes_cierre": Decimal(patrimonio),
        "bienes_inicio": Decimal(patrimonio / 2),
        "variacion_patrimonial": Decimal(patrimonio / 2) if oa else None,
        "ingresos_netos": Decimal(ingresos),
        "gastos_personales": Decimal(M),
        "detalle_bienes_cierre": Decimal(patrimonio) if oa else None,
        "inconsistente": inconsistente,
        "ingresos_inconsistentes": ingresos_inconsistentes,
        "url_fuente": "https://x",
    }


PEREZ = "20000000001"
FILAS = [
    # Pérez: tres años y, en 2024, una inicial además de la anual.
    _decl(1, "PEREZ JUAN", cuit=PEREZ, patrimonio=100 * M),
    _decl(2, "PEREZ JUAN", cuit=PEREZ, tipo="Inicial", patrimonio=7 * M),
    _decl(3, "PEREZ JUAN", cuit=PEREZ, anio=2023, patrimonio=80 * M),
    _decl(4, "PEREZ JUAN", cuit=PEREZ, anio=2022, tipo="Inicial", patrimonio=50 * M),
    _decl(
        5,
        "PEREZ JUANA",
        cuit="27000000002",
        cargo="Secretaria",
        patrimonio=2 * M,
        organismo="MINISTERIO DE ECONOMIA",
        poder="ejecutivo",
    ),
    _decl(
        6,
        "GOMEZ ANA",
        cuit="27000000003",
        cargo="Senadora Nacional",
        organismo="HONORABLE SENADO DE LA NACION",
        patrimonio=300 * M,
    ),
    _decl(7, "LOPEZ CARLOS", cuit="20000000004", patrimonio=31_000 * M, inconsistente=True),
    _decl(
        8,
        "RUIZ MARTA",
        cuit="27000000005",
        cargo="Directora",
        organismo="ARCA",
        poder="ejecutivo",
        patrimonio=5 * M,
        ingresos=900 * M,
        ingresos_inconsistentes=True,
    ),
    _decl(9, "DIAZ PEDRO", cuit="20000000006", patrimonio=-10 * M),
    # CABA 2026: sólo bienes; una persona con dos declaraciones.
    _decl(
        101,
        "PORTEÑO UNO",
        fuente=da.FUENTE_CABA,
        anio=2026,
        patrimonio=50 * M,
        cargo="Director/A General",
        poder="ejecutivo",
    ),
    _decl(
        102,
        "PORTEÑO UNO",
        fuente=da.FUENTE_CABA,
        anio=2026,
        patrimonio=50 * M,
        cargo="Director/A General",
        poder="ejecutivo",
    ),
    _decl(
        103,
        "PORTEÑA DOS",
        fuente=da.FUENTE_CABA,
        anio=2026,
        patrimonio=20 * M,
        cargo="Gerente Operativo/A",
        poder="ejecutivo",
    ),
]
BIENES = [
    {
        "dj_id": 1,
        "periodo": "cierre",
        "tipo": "INMUEBLES EN EL PAIS",
        "descripcion": "Casa",
        "titularidad": Decimal(100),
        "importe": Decimal(70 * M),
    },
    {
        "dj_id": 1,
        "periodo": "cierre",
        "tipo": "AUTOMOTORES EN EL PAIS",
        "descripcion": "Auto",
        "titularidad": Decimal(50),
        "importe": Decimal(30 * M),
    },
    {
        "dj_id": 1,
        "periodo": "inicio",
        "tipo": "INMUEBLES EN EL PAIS",
        "descripcion": "Casa",
        "titularidad": Decimal(100),
        "importe": Decimal(50 * M),
    },
]


@pytest.fixture
async def ddjj():
    engine = _engine_or_skip()
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        for tabla in (dt.TABLA_DECLARACIONES, dt.TABLA_BIENES):
            conn.execute(text(f'DROP TABLE IF EXISTS raw."{tabla}"'))
            conn.execute(text(f'CREATE TABLE raw."{tabla}" ({dt._DDL[tabla]})'))
        for fila in FILAS:
            cols = ", ".join(fila)
            conn.execute(
                text(
                    f'INSERT INTO raw."{dt.TABLA_DECLARACIONES}" ({cols}) '
                    f"VALUES ({', '.join(':' + c for c in fila)})"
                ),
                fila,
            )
        for bien in BIENES:
            conn.execute(
                text(
                    f'INSERT INTO raw."{dt.TABLA_BIENES}" (dj_id, periodo, tipo, descripcion, '
                    "titularidad, importe) VALUES (:dj_id, :periodo, :tipo, :descripcion, "
                    ":titularidad, :importe)"
                ),
                bien,
            )
        conn.execute(text(f'ANALYZE raw."{dt.TABLA_DECLARACIONES}"'))

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    url = os.environ["DATABASE_URL"].replace("+asyncpg", "+psycopg")
    async_engine = create_async_engine(url)
    yield da.DDJJAdapter(async_sessionmaker(async_engine, expire_on_commit=False))
    await async_engine.dispose()
    with engine.begin() as conn:
        for tabla in (dt.TABLA_DECLARACIONES, dt.TABLA_BIENES):
            conn.execute(text(f'DROP TABLE IF EXISTS raw."{tabla}"'))


def _nombres(result) -> list[str]:
    return [r["nombre"] for r in result.records]


async def test_ranking_excluye_la_inconsistente_y_cuenta_que_entraba(ddjj):
    result = await ddjj.ranking("patrimonio", 3)
    # Una por persona: Pérez entra con la anual (100 M), no con la inicial (7 M).
    assert _nombres(result) == ["GOMEZ ANA", "PEREZ JUAN", "RUIZ MARTA"]
    assert result.records[1]["patrimonio_cierre"] == 100 * M
    assert result.metadata["ranking"] is True and result.metadata["anio"] == 2024
    assert result.metadata["excluidas_por_inconsistencia"] == 1
    assert result.metadata["excluidas_por_inconsistencia_nombres"] == ["LOPEZ CARLOS"]
    assert "probable error de carga" in result.metadata["description"]


async def test_ranking_ascendente_no_avisa_lo_que_no_entraba(ddjj):
    result = await ddjj.ranking("patrimonio", 2, "asc")
    assert _nombres(result) == ["DIAZ PEDRO", "PEREZ JUANA"]
    assert "excluidas_por_inconsistencia" not in result.metadata


async def test_ranking_por_cargo_y_por_ingresos(ddjj):
    diputados = await ddjj.ranking("patrimonio", 10, cargo="diputado nacional")
    assert _nombres(diputados) == ["PEREZ JUAN", "DIAZ PEDRO"]
    assert diputados.metadata["excluidas_por_inconsistencia"] == 1  # López
    por_ingresos = await ddjj.ranking("ingresos", 10)
    assert "RUIZ MARTA" not in _nombres(por_ingresos)
    ejecutivo = await ddjj.ranking("patrimonio", 10, poder="ejecutivo")
    assert _nombres(ejecutivo) == ["RUIZ MARTA", "PEREZ JUANA"]
    anterior = await ddjj.ranking("patrimonio", 10, anio=2023)
    assert _nombres(anterior) == ["PEREZ JUAN"]


async def test_ranking_de_caba_es_por_bienes_y_una_por_persona(ddjj):
    result = await ddjj.ranking("patrimonio", 10, jurisdiccion="caba")
    assert _nombres(result) == ["PORTEÑO UNO", "PORTEÑA DOS"]
    assert result.metadata["anio"] == 2026
    assert result.records[0]["patrimonio_cierre"] is None
    assert result.records[0]["bienes_cierre"] == 50 * M
    assert "con mayor bienes" in result.dataset_title


async def test_estadisticas(ddjj):
    [fila] = (await ddjj.stats()).records
    # Seis personas en 2024; López afuera de los agregados.
    assert fila["total"] == 6 and fila["excluidas_por_inconsistencia"] == 1
    patrimonios = [100, 2, 300, 5, -10]
    assert fila["patrimonio_promedio"] == pytest.approx(sum(patrimonios) / 5 * M)
    assert fila["patrimonio_mediano"] == 5 * M
    assert fila["cantidad_con_patrimonio_negativo"] == 1
    assert fila["patrimonio_maximo_nombre"] == "GOMEZ ANA"
    assert fila["patrimonio_minimo_nombre"] == "DIAZ PEDRO"
    [caba] = (await ddjj.stats(jurisdiccion="caba")).records
    assert caba["total"] == 2 and caba["bienes_mediano"] == 35 * M
    assert "cantidad_con_patrimonio_negativo" not in caba and "sin deudas" in caba["nota"]


async def test_busqueda_sin_tildes_por_cuit_y_con_detalle(ddjj):
    result = await ddjj.search("juan pérez")
    # El más nuevo primero; "JUAN" también está en "JUANA".
    assert _nombres(result)[:3] == ["PEREZ JUAN", "PEREZ JUAN", "PEREZ JUANA"]
    anual = next(
        r
        for r in result.records
        if r["tipo_declaracion"] == "Anual" and r["anio_declaracion"] == 2024
    )
    assert anual["cantidad_bienes"] == 2
    assert anual["resumen_bienes"] == {"INMUEBLES": 70 * M, "AUTOMOTORES": 30 * M}
    assert anual["bienes_detalle"][1]["titularidad"] == "50%"
    por_cuit = await ddjj.search("20-00000000-1")
    assert set(_nombres(por_cuit)) == {"PEREZ JUAN"} and len(por_cuit.records) == 4
    # La ñ cuenta como n en los dos lados: "porteñ" encuentra a PORTEÑO y a PORTEÑA.
    caba = await ddjj.search("porteñ", jurisdiccion="caba")
    assert _nombres(caba) == ["PORTEÑA DOS", "PORTEÑO UNO", "PORTEÑO UNO"]
    vacia = await ddjj.search("nadie existe")
    assert vacia.records == []
    assert "Oficina Anticorrupción 2022–2024" in vacia.metadata["cobertura"]
    assert "Ciudad de Buenos Aires 2026–2026" in vacia.metadata["cobertura"]


async def test_evolucion(ddjj):
    homonimos = await ddjj.evolucion("perez juan")
    assert homonimos.metadata["varias_personas"] is True
    assert {r["nombre"] for r in homonimos.records} == {"PEREZ JUAN", "PEREZ JUANA"}
    result = await ddjj.evolucion(PEREZ)
    assert [r["anio"] for r in result.records] == [2022, 2023, 2024]
    # En 2024 cuenta la anual, no la inicial.
    assert [r["patrimonio_cierre"] for r in result.records] == [50 * M, 80 * M, 100 * M]
    assert "sin ajustar por inflación" in result.metadata["description"]


async def test_cobertura_y_conteo(ddjj):
    assert await ddjj.cobertura() == {da.FUENTE_OA: (2022, 2024), da.FUENTE_CABA: (2026, 2026)}
    assert await ddjj.contar() == len(FILAS)
