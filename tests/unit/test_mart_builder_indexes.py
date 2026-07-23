"""Tests for the automatic-index generation in `marts.builder.build_index_sql`.

Verified on staging that 30 of 30 marts >500K rows had zero indexes
before this change — every query against them ran a parallel sequential
scan over the full materialized view. The patterns here are tuned
against the columns those marts actually carry (verified by iterating
`mart_definitions.canonical_columns_json`).
"""

from __future__ import annotations

from app.application.marts.builder import (
    _is_indexable_column,
    build_create_view_sql,
    build_index_sql,
)
from app.application.marts.mart import (
    Mart,
    MartCanonicalColumn,
    MartRefreshPolicy,
)


def _mart(view: str, cols: list[tuple[str, str]], unique_index: list[str] | None = None) -> Mart:
    return Mart(
        id=view,
        version="0.1.0",
        description="t",
        domain="test",
        source_portals=[],
        source_resource_patterns=[],
        canonical_columns=[MartCanonicalColumn(name=n, type=t, description="") for n, t in cols],
        sql="SELECT 1",
        refresh=MartRefreshPolicy(policy="manual", unique_index=unique_index or []),
    )


class TestIndexableColumn:
    def test_exact_temporal(self) -> None:
        for name in ["fecha", "anio", "año", "periodo", "mes", "ejercicio", "year", "date"]:
            assert _is_indexable_column(name), name

    def test_prefixed_temporal(self) -> None:
        for name in ["fecha_de_inicio", "fecha_actualizacion", "fecha_contrato_social"]:
            assert _is_indexable_column(name), name

    def test_suffixed_temporal(self) -> None:
        for name in ["tramite_fecha", "fecha_ultima_audiencia"]:
            assert _is_indexable_column(name), name

    def test_exact_geographic(self) -> None:
        for name in ["provincia", "barrio", "comuna", "departamento", "municipio", "jurisdiccion"]:
            assert _is_indexable_column(name), name

    def test_geographic_with_suffix(self) -> None:
        for name in ["provincia_nombre", "provincia_id", "departamento_desc"]:
            assert _is_indexable_column(name), name

    def test_geographic_embedded(self) -> None:
        for name in ["dom_fiscal_provincia", "dom_legal_provincia"]:
            assert _is_indexable_column(name), name

    def test_non_indexable_columns(self) -> None:
        for name in ["nombre", "cuit", "valor", "monto", "id_caso", "codigo_delito", "tentativa"]:
            assert not _is_indexable_column(name), name

    def test_semantic_filter_columns(self) -> None:
        # Verified hot via pg_stat_statements in v4.6 migration.
        for name in [
            "tipo",
            "subtipo",
            "categoria",
            "categoría",
            "estado",
            "fuero",
            "franja",
            "estacion",
            "estación",
            "tipo_de_mediacion",
            "resultado_mediacion",
            "tipo_vehiculo",
            "sector",
            "rubro",
            "tipo_persona",
            "estado_civil",
        ]:
            assert _is_indexable_column(name), name

    def test_semantic_does_not_overmatch(self) -> None:
        # These should NOT be flagged — too generic / not actual discriminators.
        for name in [
            "descripcion",
            "observaciones",
            "comentarios",
            "url",
            "link",
            "telefono",
            "email",
            "razon_social",  # text-search candidate, separate trigram path
        ]:
            assert not _is_indexable_column(name), name


class TestBuildIndexSql:
    def test_emits_index_per_matched_column(self) -> None:
        mart = _mart(
            "delitos_caba",
            [
                ("anio", "text"),
                ("mes", "text"),
                ("fecha", "text"),
                ("barrio", "text"),
                ("comuna", "text"),
                ("tipo", "text"),
            ],
        )
        sql_list = build_index_sql(mart)
        # 6 indexable cols including `tipo` (semantic pattern added in v4.6).
        assert len(sql_list) == 6
        assert any('"ix_delitos_caba_fecha"' in s for s in sql_list)
        assert any('"ix_delitos_caba_barrio"' in s for s in sql_list)
        assert any('"ix_delitos_caba_tipo"' in s for s in sql_list)
        assert all("IF NOT EXISTS" in s for s in sql_list)
        assert all(' ON mart."delitos_caba" ' in s for s in sql_list)

    def test_no_match_returns_empty(self) -> None:
        mart = _mart(
            "archivos_judiciales",
            [("id_caso", "text"), ("codigo_delito", "text"), ("tentativa", "text")],
        )
        assert build_index_sql(mart) == []

    def test_skips_columns_already_in_unique_index(self) -> None:
        mart = _mart(
            "presupuesto_test",
            [("anio", "text"), ("provincia", "text"), ("monto", "numeric")],
            unique_index=["anio", "provincia"],
        )
        # both anio and provincia would normally be indexed, but the unique
        # index already covers them — build_index_sql should not duplicate.
        result = build_index_sql(mart)
        assert result == []

    def test_index_name_truncated_to_63_chars(self) -> None:
        long_view = "a" * 80
        mart = _mart(long_view, [("fecha", "text")])
        sql = build_index_sql(mart)[0]
        # Extract the quoted index name and check length
        idx_name = sql.split('"')[1]
        assert len(idx_name) <= 63

    def test_full_create_view_chains_indexes(self) -> None:
        mart = _mart(
            "flujo_vehicular_peajes_caba",
            [("fecha", "text"), ("periodo", "text"), ("estacion", "text")],
        )
        out = build_create_view_sql(mart)
        # 1 DROP + 1 CREATE VIEW + 3 indexes (fecha + periodo + estacion).
        # `estacion` matches the semantic pattern added in v4.6.
        assert sum(1 for s in out if s.startswith("DROP MATERIALIZED VIEW")) == 1
        assert sum(1 for s in out if s.startswith("CREATE MATERIALIZED VIEW")) == 1
        assert sum(1 for s in out if s.startswith("CREATE INDEX")) == 3
        assert any('"ix_flujo_vehicular_peajes_caba_fecha"' in s for s in out)
        assert any('"ix_flujo_vehicular_peajes_caba_estacion"' in s for s in out)
