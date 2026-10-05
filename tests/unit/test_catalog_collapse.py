"""`collapse_hits`: una entrada por archivo y la copia que se muestra.

Los casos salen de prod (04-oct): los gemelos de "Reservas internacionales"
de la migración de datos.gob.ar, el CSV y el JSON de Votaciones Nominales y de
Proyectos Parlamentarios (el CSV con el encabezado roto), los espejos entre
portales y las copias cortadas en el tope de filas.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.application.catalog.collapse import collapse_hits, header_looks_like_data
from app.application.catalog.national_prior import (
    asks_for_national,
    names_a_place,
    national_prior,
)
from app.domain.ports.sandbox.sql_sandbox import CachedTableInfo, TableProfile
from app.domain.ports.search.vector_search import SearchResult

_OLD = datetime(2026, 5, 4, tzinfo=UTC)
_NEW = datetime(2026, 7, 29, tzinfo=UTC)

_R922 = (
    "https://infra.datos.gob.ar/catalog/sspm/dataset/92/distribution/92.2/download/"
    "reservas-internacionales-pasivos-financieros-bcra.csv"
)
_R921 = _R922.replace("92.2", "92.1")
_HCDN = "https://datos.hcdn.gob.ar:443/dataset/2e08ab84/resource/{rid}/download/{file}"


def _hit(ds: str, title: str, url: str, score: float, portal: str = "datos_gob_ar"):
    return SearchResult(
        dataset_id=ds,
        title=title,
        description="",
        portal=portal,
        download_url=url,
        columns="",
        score=score,
    )


def _table(name: str, ds: str, cd_rows: int | None, columns: list[str] | None = None):
    return CachedTableInfo(
        table_name=f"raw.{name}", dataset_id=ds, row_count=cd_rows, columns=columns or []
    )


def _profile(name: str, rows: int, *, created=_NEW, fmt="csv", truncated=False, columns=()):
    return TableProfile(
        table_name=name,
        rows=rows,
        truncated=truncated,
        dataset_created_at=created,
        format=fmt,
        columns=list(columns),
    )


def test_migration_twins_show_one_table_the_one_with_more_real_rows() -> None:
    """Antes: un resultado con las dos tablas (8.467 y 8.582) y el modelo adivina."""
    hits = [
        _hit("old", "Reservas internacionales y pasivos del BCRA", _R922, 0.766),
        _hit("new", "Reservas internacionales y pasivos del BCRA", _R922, 0.765),
    ]
    tables = [_table("reservas__bebd015a__v1", "old", 0), _table("reservas_rf3719", "new", 8582)]
    profiles = {
        "reservas__bebd015a__v1": _profile("reservas__bebd015a__v1", 8467, created=_OLD),
        "reservas_rf3719": _profile("reservas_rf3719", 8582, created=_NEW),
    }

    [only] = collapse_hits(hits, tables, profiles)

    assert only.hit.dataset_id == "new"
    assert [(t.table_name, t.row_count) for t in only.tables] == [("raw.reservas_rf3719", 8582)]
    assert only.copies == 2
    assert only.score == pytest.approx(0.766)  # el mejor del grupo


def test_the_new_era_does_not_win_when_it_has_fewer_rows() -> None:
    """153 gemelos de prod tienen la tabla nueva más chica que la vieja."""
    hits = [_hit("new", "T", _R922, 0.7), _hit("old", "T", _R922, 0.7)]
    tables = [_table("n", "new", 100), _table("o", "old", 120)]
    profiles = {"n": _profile("n", 100, created=_NEW), "o": _profile("o", 120, created=_OLD)}

    [only] = collapse_hits(hits, tables, profiles)
    assert only.hit.dataset_id == "old"


def test_a_copy_cut_at_the_row_cap_loses_to_the_complete_one() -> None:
    hits = [_hit("new", "IGJ entidades", _R922, 0.7), _hit("old", "IGJ entidades", _R922, 0.69)]
    tables = [_table("n", "new", 500_000), _table("o", "old", 480_000)]
    profiles = {
        "n": _profile("n", 500_000, created=_NEW, truncated=True),
        "o": _profile("o", 480_000, created=_OLD),
    }

    [only] = collapse_hits(hits, tables, profiles)
    assert only.hit.dataset_id == "old"


def test_row_cap_is_recognized_even_without_the_flag() -> None:
    hits = [_hit("new", "X", _R922, 0.7), _hit("old", "X", _R922, 0.7)]
    tables = [_table("n", "new", 2_500_000), _table("o", "old", 1_900_000)]
    profiles = {"n": _profile("n", 2_500_000), "o": _profile("o", 1_900_000, created=_OLD)}

    [only] = collapse_hits(hits, tables, profiles)
    assert only.hit.dataset_id == "old"


def test_csv_and_json_of_the_same_file_are_one_result_and_the_csv_wins_a_tie() -> None:
    csv = _HCDN.format(rid="r1", file="detalle-actas-datos-generales-2.4.csv")
    json_ = _HCDN.format(rid="r2", file="detalle-actas-datos-generales-2.4.json")
    hits = [
        _hit("json", "Votaciones Nominales", json_, 0.675, "diputados"),
        _hit("csv", "Votaciones Nominales", csv, 0.674, "diputados"),
    ]
    tables = [_table("vn_json", "json", 0), _table("vn_csv", "csv", 0)]
    profiles = {
        "vn_json": _profile("vn_json", 231_043, fmt="json"),
        "vn_csv": _profile("vn_csv", 231_043, fmt="csv"),
    }

    [only] = collapse_hits(hits, tables, profiles)

    assert only.hit.dataset_id == "csv"
    assert only.archivo == "detalle-actas-datos-generales-2.4.csv"
    assert only.formato == "CSV"


def test_a_broken_csv_loses_to_its_json_even_when_the_registry_lies_about_rows() -> None:
    """Proyectos Parlamentarios: el CSV quedó con una fila de datos de
    encabezado y `cached_datasets` le asigna 111.091 filas (tiene 11.089)."""
    csv = _HCDN.format(rid="r1", file="proyectos_parlamentarios2.5.csv")
    json_ = _HCDN.format(rid="r2", file="proyectos_parlamentarios2.5.json")
    broken = ["hcdn110412 / hcdn110416", "CONCURSOS Y QUIEBRAS - MODIFICACION DE LA LEY 24522 Y", "2009-11-12T00:00:00", "hcdn127tp158"]  # fmt: skip
    hits = [
        _hit("csv", "Proyectos Parlamentarios", csv, 0.678, "diputados"),
        _hit("json", "Proyectos Parlamentarios", json_, 0.672, "diputados"),
    ]
    tables = [
        _table("pp_csv", "csv", 111_091, broken),
        _table("pp_json", "json", 111_091, ["proyecto_id", "titulo", "publicacion_fecha"]),
    ]

    # Sin el perfil (sandbox viejo): la cuenta de filas empata y decide el encabezado.
    [only] = collapse_hits(hits, tables, {})
    assert only.hit.dataset_id == "json"

    # Con el perfil: también por filas reales.
    profiles = {"pp_csv": _profile("pp_csv", 11_089), "pp_json": _profile("pp_json", 111_091)}
    [only] = collapse_hits(hits, tables, profiles)
    assert only.hit.dataset_id == "json"
    assert only.tables[0].row_count == 111_091


def test_same_file_name_in_two_distributions_is_not_merged() -> None:
    """92.1 (mensual) y 92.2 (diaria) comparten nombre de archivo y extensión."""
    hits = [_hit("d", "Reservas", _R922, 0.76), _hit("m", "Reservas", _R921, 0.75)]
    tables = [_table("d", "d", 8582), _table("m", "m", 282)]

    out = collapse_hits(hits, tables, {})
    assert [r.hit.dataset_id for r in out] == ["d", "m"]


def test_two_csvs_with_the_same_name_do_not_pull_in_a_json() -> None:
    """Si una extensión se repite en el package, no se sabe qué va con qué."""
    a = _HCDN.format(rid="r1", file="datos.csv")
    b = _HCDN.format(rid="r2", file="datos.csv")
    c = _HCDN.format(rid="r3", file="datos.json")
    hits = [_hit("a", "X", a, 0.7), _hit("b", "X", b, 0.69), _hit("c", "X", c, 0.68)]
    tables = [_table("a", "a", 10), _table("b", "b", 20), _table("c", "c", 10)]

    assert len(collapse_hits(hits, tables, {})) == 3


def test_mirrors_across_portals_are_one_result() -> None:
    url = "http://datos.energia.gob.ar/dataset/x/resource/y/download/operadores-glp.csv"
    hits = [
        _hit("a", "Operadores de GLP", url, 0.7),
        _hit("b", "Operadores de GLP", url, 0.7, "energia"),
    ]
    tables = [_table("a", "a", 7), _table("b", "b", 7)]

    [only] = collapse_hits(hits, tables, {})
    assert only.copies == 2


def test_url_spelling_differences_do_not_split_a_file() -> None:
    hits = [
        _hit("a", "T", "https://datos.hcdn.gob.ar:443/dataset/p/resource/r/download/f.csv", 0.7),
        _hit("b", "T", "https://DATOS.hcdn.gob.ar/dataset/p/resource/r/download/f.csv/", 0.7),
    ]
    assert len(collapse_hits(hits, [_table("a", "a", 1)], {})) == 1


def test_twins_with_a_different_url_merge_only_with_the_same_file_and_shape() -> None:
    cols = ["mesa", "partido", "votos"]
    hits = [
        _hit("a", "Elecciones 2019", "https://a.gob.ar/x/gobernador.csv", 0.7, "entre_rios"),
        _hit("b", "Elecciones 2019", "https://b.gob.ar/y/gobernador.csv", 0.7, "entre_rios"),
        # Otra categoría: misma forma (una fila por mesa), otro archivo.
        _hit("c", "Elecciones 2019", "https://a.gob.ar/x/diputados.csv", 0.7, "entre_rios"),
    ]
    tables = [_table("a", "a", 900, cols), _table("b", "b", 900, cols), _table("c", "c", 900, cols)]

    out = collapse_hits(hits, tables, {})
    assert sorted(r.copies for r in out) == [1, 2]


def test_a_copy_with_a_table_beats_one_without() -> None:
    url = _HCDN.format(rid="r", file="actas.csv")
    hits = [_hit("sin", "T", url, 0.8), _hit("con", "T", url, 0.7)]

    [only] = collapse_hits(hits, [_table("con", "con", 899)], {})
    assert only.hit.dataset_id == "con"


def test_tables_without_real_rows_are_not_offered() -> None:
    hits = [_hit("a", "T", "", 0.7)]
    out = collapse_hits(hits, [_table("vacia", "a", 0)], {"vacia": _profile("vacia", 0)})
    assert out[0].tables == []


def test_staging_zero_row_count_is_overridden_by_the_live_version() -> None:
    """Staging: `cached_datasets.row_count` = 0 en 18.134 tablas con filas."""
    hits = [_hit("a", "Votaciones Nominales", "", 0.7, "diputados")]
    out = collapse_hits(hits, [_table("vn", "a", 0)], {"vn": _profile("vn", 231_043)})
    assert out[0].tables[0].row_count == 231_043


def test_without_collapsing_order_follows_score() -> None:
    hits = [_hit("a", "A", "https://x/a.csv", 0.8), _hit("b", "B", "https://x/b.csv", 0.7)]
    out = collapse_hits(hits, [], {})
    assert [r.hit.dataset_id for r in out] == ["a", "b"]
    assert [r.archivo for r in out] == ["a.csv", "b.csv"]


# ── encabezados ─────────────────────────────────────────────


@pytest.mark.parametrize(
    "columns",
    [
        [
            "hcdn110412 / hcdn110416",
            "CONCURSOS Y QUIEBRAS - MODIFICACION DE LA LEY 24522",
            "2009-11-12T00:00:00",
            "hcdn127tp158",
        ],  # fmt: skip
        ["12/11/2009", "1.234.567", "x"],
    ],
)
def test_data_rows_as_headers_are_noticed(columns: list[str]) -> None:
    assert header_looks_like_data(columns)


@pytest.mark.parametrize(
    "columns",
    [
        ["proyecto_id", "titulo", "publicacion_fecha", "_source_url"],
        ["provincia", "2019", "2020", "2021"],  # tabla ancha del INDEC
        ["indice_tiempo", "tipo_cambio_bna_vendedor"],
        ["x"],
        [],
    ],
)
def test_ordinary_headers_are_not_flagged(columns: list[str]) -> None:
    assert not header_looks_like_data(columns)


# ── prioridad nacional ──────────────────────────────────────


def test_national_prior_breaks_a_near_tie_toward_the_national_dataset() -> None:
    """ "deuda pública nacional": Mendoza 0,649 sobre Títulos Públicos 0,644."""
    hits = [
        _hit("mza", "Deuda Pública 2017", "https://m/2017.csv", 0.649, "mendoza"),
        _hit("nac", "Títulos Públicos de Deuda", "https://n/t.csv", 0.644, "datos_gob_ar"),
    ]
    q = "deuda pública nacional"

    out = collapse_hits(hits, [], {}, prior=national_prior(q))
    assert [r.hit.dataset_id for r in out] == ["nac", "mza"]
    # Sin prior, el orden era el del coseno.
    assert [r.hit.dataset_id for r in collapse_hits(hits, [], {})] == ["mza", "nac"]


def test_no_national_prior_when_the_query_names_a_place() -> None:
    hits = [
        _hit("cba", "Homicidios Córdoba", "https://c/h.csv", 0.553, "cordoba_prov"),
        _hit("nac", "SNIC homicidios", "https://n/s.csv", 0.54, "datos_gob_ar"),
    ]
    out = collapse_hits(hits, [], {}, prior=national_prior("homicidios en Córdoba"))
    assert [r.hit.dataset_id for r in out] == ["cba", "nac"]


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("homicidios en Córdoba", True),
        ("presupuesto de la Ciudad de Buenos Aires", True),
        ("licencias de conducir en Mendoza", True),
        ("gasto del municipio de Pinamar", True),
        ("matrícula escolar en CABA", True),
        ("homicidios dolosos por provincia", False),
        ("matrícula escolar por provincia", False),
        ("deuda pública nacional", False),
        ("tasa de desempleo", False),
        ("exportaciones de soja", False),
    ],
)
def test_names_a_place(query: str, expected: bool) -> None:
    assert names_a_place(query) is expected


def test_asking_for_national_doubles_the_prior() -> None:
    """Prod, 05-oct: "Puestos de trabajo en la APN" (0,629) sólo pasa al listado
    de agentes de Córdoba (0,663) con 0,04, que sería ruido para el resto."""
    hits = [
        _hit(
            "cba",
            "Listado de agentes del Poder Ejecutivo",
            "https://c/a.csv",
            0.663,
            "cordoba_prov",
        ),
        _hit("apn", "Puestos de trabajo en la APN", "https://n/p.csv", 0.629, "datos_gob_ar"),
    ]
    explicit = collapse_hits(
        hits, [], {}, prior=national_prior("cantidad de empleados públicos nacionales")
    )
    implicit = collapse_hits(hits, [], {}, prior=national_prior("cantidad de empleados públicos"))

    assert [r.hit.dataset_id for r in explicit] == ["apn", "cba"]
    assert [r.hit.dataset_id for r in implicit] == ["cba", "apn"]


@pytest.mark.parametrize(
    ("query", "expected"),
    [
        ("deuda pública nacional", True),
        ("inflación en la Argentina", True),
        ("cuánto gasta la Nación en universidades", True),
        ("tasa de desempleo", False),
        ("coparticipación federal", False),
    ],
)
def test_asks_for_national(query: str, expected: bool) -> None:
    assert asks_for_national(query) is expected


# ── el perfil que trae el sandbox ───────────────────────────


def test_sandbox_profiles_map_the_live_version_by_bare_name() -> None:
    from types import SimpleNamespace

    from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter

    captured: dict = {}
    row = SimpleNamespace(
        table_name="diputados__proyectos_parlamentarios__40eec388__v1",
        row_count=11089,
        is_truncated=False,
        loaded_at=_OLD,
        dataset_created_at=_OLD,
        format="csv",
        column_names=["hcdn110412 / hcdn110416", "_source_url"],
    )

    class _Conn:
        def __enter__(self):
            return self

        def __exit__(self, *_a) -> bool:
            return False

        def execute(self, statement, params=None):
            captured["sql"], captured["params"] = str(statement), params
            return SimpleNamespace(fetchall=lambda: [row])

        def rollback(self) -> None:
            captured["rolled_back"] = True

    adapter = PgSandboxAdapter()
    adapter._get_engine = lambda: SimpleNamespace(connect=_Conn)  # type: ignore[method-assign]

    out = adapter._table_profiles_sync(["raw.diputados__proyectos_parlamentarios__40eec388__v1"])

    assert captured["params"] == {"names": ["diputados__proyectos_parlamentarios__40eec388__v1"]}
    assert "raw_table_versions" in captured["sql"] and "superseded_at IS NULL" in captured["sql"]
    assert captured["rolled_back"]
    profile = out["diputados__proyectos_parlamentarios__40eec388__v1"]
    assert (profile.rows, profile.truncated, profile.format) == (11089, False, "csv")
    assert profile.columns == ["hcdn110412 / hcdn110416", "_source_url"]


def test_sandbox_profiles_without_names_do_not_touch_the_database() -> None:
    from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter

    adapter = PgSandboxAdapter()
    adapter._get_engine = lambda: (_ for _ in ()).throw(AssertionError("no DB"))  # type: ignore[method-assign]
    assert adapter._table_profiles_sync([]) == {}
