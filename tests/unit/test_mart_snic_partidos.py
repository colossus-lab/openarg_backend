"""Un partido, una fila por año y delito, en el SQL que arma el mart del SNIC.

H004 (revisión independiente del 05-oct): `mart.delitos_argentina_snic` servía
cada partido de Buenos Aires y cada comuna de CABA dos veces, en todos los años
desde 2000. "Homicidios dolosos en PBA 2019" daba 1.754 contra 877 reales, y el
total de 2019, 3.058 contra 2.085. Ya salía así en prod.

La causa está en las fuentes, no en el `DISTINCT ON`: el macro une dos copias
del mismo archivo —el CSV y el XLSX de la publicación de mayo de 2026— y el
XLSX se cargó con los códigos como `bigint`. Al pasar a texto, Buenos Aires es
'6' en una copia y '06' en la otra, y el partido 25 de Mayo, '6854' y '06854'.
La deduplicación comparaba el código crudo y no reconocía las dos copias. Las
provincias con código de dos cifras (Córdoba, '14') no tienen cero adelante y
por eso salían bien, que es lo que lo hacía difícil de ver.

Estos tests corren el SQL que resuelve `build_mart` contra Postgres de verdad,
con las dos copias reproducidas tal como están en staging: una con texto y
ceros, otra con enteros. Las tablas son `VALUES`, no tablas creadas, así que
todo corre en una transacción de sólo lectura: el test no escribe nada aunque
`DATABASE_URL` apunte a una base real. Sin base disponible, se saltean.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from app.application.marts.mart import load_mart
from app.application.marts.sql_macros import _LiveRow, resolve_macros

_YAML = Path(__file__).resolve().parents[2] / "config" / "marts" / "delitos_argentina_snic.yaml"

# Los dos recursos vivos que el patrón del mart une hoy en staging y en prod
# (los otros dos que matchea no traen `cod_delito` y `require_all_columns` los
# descarta). Nombres reales, para que el SQL resuelto sea el de verdad.
_CSV = "datos_gob_ar__snic_departamental_estadisticas_cri__4bf053c0__v1"
_XLSX = "datos_gob_ar__snic_departamental_estadisticas_cri__ae856597__v1"

_NUMERICAS = (
    "cantidad_victimas",
    "cantidad_victimas_masc",
    "cantidad_victimas_fem",
    "cantidad_victimas_sd",
    "tasa_hechos",
    "tasa_victimas",
    "tasa_victimas_masc",
    "tasa_victimas_fem",
)

# Tipos de cada copia, medidos en staging (`information_schema.columns`).
_TIPOS_CSV = {
    "provincia_id": "text",
    "provincia_nombre": "text",
    "departamento_id": "text",
    "departamento_nombre": "text",
    "anio": "text",
    "codigo_delito_snic_id": "text",
    "cod_delito": "text",
    "codigo_delito_snic_nombre": "text",
    "cantidad_hechos": "text",
    **{c: "text" for c in _NUMERICAS},
    "_source_dataset_id": "text",
}
_TIPOS_XLSX = {
    "provincia_id": "bigint",
    "provincia_nombre": "text",
    "departamento_id": "bigint",
    "departamento_nombre": "text",
    "anio": "bigint",
    "codigo_delito_snic_id": "text",
    "cod_delito": "bigint",
    "codigo_delito_snic_nombre": "text",
    "cantidad_hechos": "bigint",
    **{c: "double precision" for c in _NUMERICAS},
    "_source_dataset_id": "text",
}

# (provincia, nombre, departamento, nombre, año, código, delito, hechos)
_FILAS = [
    (6, "Buenos Aires", 6854, "25 de Mayo", 2019, 1, "Homicidios dolosos", 3),
    (
        6,
        "Buenos Aires",
        6854,
        "25 de Mayo",
        2019,
        2,
        "Homicidios dolosos en grado de tentativa",
        2,
    ),
    (2, "Ciudad Autónoma de Buenos Aires", 2007, "Comuna 7", 2019, 1, "Homicidios dolosos", 9),
    # Sin cero adelante: las dos copias ya coincidían y el mart lo hacía bien.
    (14, "Córdoba", 14014, "Capital", 2019, 1, "Homicidios dolosos", 40),
    # La provincia donde el tope de 500.000 filas corta el archivo.
    (86, "Santiago del Estero", 86112, "Mitre", 2019, 1, "Homicidios dolosos", 1),
]


def _literal(valor: object, tipo: str) -> str:
    if valor is None:
        return f"NULL::{tipo}"
    if tipo == "text":
        return "'" + str(valor).replace("'", "''") + "'::text"
    return f"{valor}::{tipo}"


def _como_csv(fila: tuple) -> dict[str, object]:
    prov, prov_nombre, depto, depto_nombre, anio, cod, delito, hechos = fila
    return {
        "provincia_id": f"{prov:02d}",
        "provincia_nombre": prov_nombre,
        "departamento_id": f"{depto:05d}",
        "departamento_nombre": depto_nombre,
        "anio": str(anio),
        "codigo_delito_snic_id": str(cod),
        "cod_delito": str(cod),
        "codigo_delito_snic_nombre": delito,
        "cantidad_hechos": str(hechos),
        **{c: "0" for c in _NUMERICAS},
        "_source_dataset_id": "seguridad_3.2",
    }


def _como_xlsx(fila: tuple) -> dict[str, object]:
    prov, prov_nombre, depto, depto_nombre, anio, cod, delito, hechos = fila
    return {
        "provincia_id": prov,
        "provincia_nombre": prov_nombre,
        "departamento_id": depto,
        "departamento_nombre": depto_nombre,
        "anio": anio,
        "codigo_delito_snic_id": str(cod),
        "cod_delito": cod,
        "codigo_delito_snic_nombre": delito,
        "cantidad_hechos": hechos,
        **{c: 0 for c in _NUMERICAS},
        "_source_dataset_id": "seguridad_3.1",
    }


def _valores(tipos: dict[str, str], filas: list[dict[str, object]]) -> str:
    """Una "tabla" como `(VALUES ...) AS t(...)`, con los tipos de la real."""
    columnas = list(tipos)
    cuerpo = ", ".join(
        "(" + ", ".join(_literal(f[c], tipos[c]) for c in columnas) + ")" for f in filas
    )
    nombres = ", ".join(f'"{c}"' for c in columnas)
    return f"(VALUES {cuerpo}) AS t({nombres})"


_FUENTES = {
    _CSV: (_TIPOS_CSV, _valores(_TIPOS_CSV, [_como_csv(f) for f in _FILAS])),
    _XLSX: (_TIPOS_XLSX, _valores(_TIPOS_XLSX, [_como_xlsx(f) for f in _FILAS])),
}


def _sql_resuelto() -> str:
    """El SQL que `build_mart` ejecutaría con estas dos fuentes vivas.

    Se reemplazan sólo las lecturas del registro y el nombre físico de cada
    tabla; la proyección del macro, sus filtros y el SQL del YAML son los
    reales.
    """
    vivas = [
        _LiveRow(resource_identity=f"datos_gob_ar::{n}", schema_name="raw", table_name=n)
        for n in (_CSV, _XLSX)
    ]
    modulo = "app.application.marts.sql_macros"
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(f"{modulo}._query_live_identities", lambda _e, _i: {})
        mp.setattr(f"{modulo}._query_live_by_portals", lambda _e, _p: [])
        mp.setattr(f"{modulo}._query_live_by_identity_patterns", lambda _e, _p: [])
        mp.setattr(f"{modulo}._query_live_by_table_patterns", lambda _e, _p: vivas)
        mp.setattr(
            f"{modulo}._query_columns",
            lambda _e, pares: {p: set(_FUENTES[p[1]][0]) for p in pares},
        )
        mp.setattr(f"{modulo}._qualified", lambda r: _FUENTES[r.table_name][1])
        return resolve_macros(load_mart(_YAML).sql, engine=object())


def _engine_or_skip():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        pytest.skip("DATABASE_URL not set — este test necesita Postgres")
    try:
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        return engine
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")


@pytest.fixture(scope="module")
def filas() -> list[dict]:
    engine = _engine_or_skip()
    sql = _sql_resuelto()
    with engine.connect() as conn:
        conn.execute(text("SET TRANSACTION READ ONLY"))
        resultado = [dict(r) for r in conn.execute(text(sql)).mappings().all()]
        conn.rollback()
    return resultado


def _partido(fila: dict) -> tuple[str, str]:
    """La identidad de un partido sin importar cómo venga escrito el código."""
    return (str(fila["provincia_id"]).lstrip("0"), str(fila["departamento_id"]).lstrip("0"))


def test_un_partido_una_fila_por_anio_y_delito(filas: list[dict]) -> None:
    """El invariante del mart: (partido, año, delito) no se repite."""
    claves = [(*_partido(f), f["anio"], f["cod_delito"]) for f in filas]
    repetidas = sorted({c for c in claves if claves.count(c) > 1})
    assert not repetidas, (
        f"{len(claves) - len(set(claves))} filas repetidas para la misma clave: {repetidas}. "
        "Dos copias del mismo partido con el código escrito distinto ('06854' y '6854')."
    )


def test_homicidios_de_pba_no_se_cuentan_dos_veces(filas: list[dict]) -> None:
    total = sum(
        f["cantidad_hechos"]
        for f in filas
        if f["provincia_nombre"] == "Buenos Aires"
        and f["anio"] == "2019"
        and f["codigo_delito_snic_nombre"] == "Homicidios dolosos"
    )
    assert total == 3


def test_los_codigos_salen_en_formato_indec(filas: list[dict]) -> None:
    """Provincia de dos cifras y departamento de cinco, con el cero adelante:
    es como los publica el SNIC y como los busca quien filtra por código."""
    codigos = {(f["provincia_id"], f["departamento_id"]) for f in filas}
    assert codigos == {("06", "06854"), ("02", "02007"), ("14", "14014")}


def test_delitos_distintos_del_mismo_partido_no_se_colapsan(filas: list[dict]) -> None:
    """Deduplicar no puede llevarse puesta la tentativa: es otro delito."""
    delitos = sorted(
        f["codigo_delito_snic_nombre"] for f in filas if f["departamento_id"] == "06854"
    )
    assert delitos == ["Homicidios dolosos", "Homicidios dolosos en grado de tentativa"]


def test_la_provincia_que_corta_el_tope_no_se_sirve(filas: list[dict]) -> None:
    """Todas las fuentes están truncadas en 500.000 filas y el archivo viene
    ordenado por provincia: Santiago del Estero llega con 16 de sus 27
    departamentos. Un total provincial con la mitad de los departamentos es un
    dato falso; el mart no lo sirve."""
    assert not [f for f in filas if _partido(f)[0] == "86"]


def test_la_version_subio() -> None:
    """`build_mart` rechaza reconstruir con un YAML de versión menor a la
    registrada. Sin subirla, un worker con la imagen vieja podría volver a
    armar el mart duplicado después del arreglo."""
    assert load_mart(_YAML).version != "0.2.0"


def test_el_agente_ve_el_aviso_de_cobertura() -> None:
    """`buscar_datos` le muestra al agente sólo el principio de la descripción.
    El aviso de que faltan provincias tiene que entrar ahí, o el agente suma
    las 21 que hay y lo presenta como el total del país."""
    from app.application.answers.tools.catalogo import _DESCRIPTION_CHARS

    visible = load_mart(_YAML).description[:_DESCRIPTION_CHARS]
    assert "Tucumán" in visible
    assert "total nacional" in visible
