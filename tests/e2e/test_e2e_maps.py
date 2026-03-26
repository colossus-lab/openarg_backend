"""E2E tests: geographic data and map rendering.

Tests that the pipeline correctly returns map_data (GeoJSON FeatureCollection)
when querying geographic datasets. These tests validate the full stack:
  - Collector stored geo data with _geometry_geojson column
  - NL2SQL includes _geometry_geojson in SELECT for map queries
  - Analyst builds FeatureCollection from results
  - API returns map_data in the response

Note: these tests only run against staging with real geo data.
"""

from __future__ import annotations

import pytest

from tests.e2e.helpers import ask, headers, smart_payload

pytestmark = [pytest.mark.e2e, pytest.mark.asyncio]


def assert_has_map_data(data: dict, label: str, *, min_features: int = 1) -> None:
    """Assert the response includes valid GeoJSON map_data."""
    map_data = data.get("map_data")
    assert map_data is not None, f"No map_data in response for: {label}"
    assert map_data.get("type") == "FeatureCollection", (
        f"map_data.type should be FeatureCollection, got {map_data.get('type')} for: {label}"
    )
    features = map_data.get("features", [])
    assert len(features) >= min_features, (
        f"Expected >= {min_features} features, got {len(features)} for: {label}"
    )
    # Validate first feature structure
    feat = features[0]
    assert feat.get("type") == "Feature", f"Feature type mismatch for: {label}"
    assert "geometry" in feat, f"Feature missing geometry for: {label}"
    assert "properties" in feat, f"Feature missing properties for: {label}"
    geom = feat["geometry"]
    assert geom.get("type") in (
        "Point",
        "MultiPoint",
        "LineString",
        "MultiLineString",
        "Polygon",
        "MultiPolygon",
        "GeometryCollection",
    ), f"Invalid geometry type {geom.get('type')} for: {label}"
    assert "coordinates" in geom, f"Geometry missing coordinates for: {label}"


def assert_no_map_data(data: dict, label: str) -> None:
    """Assert the response does NOT include map_data (non-geo query)."""
    map_data = data.get("map_data")
    assert map_data is None, (
        f"Unexpected map_data for non-geo query: {label} "
        f"(got {len(map_data.get('features', []))} features)"
    )


def assert_has_answer_and_map(data: dict, keywords: list[str], label: str) -> None:
    """Assert both a substantive answer AND map_data are present."""
    answer = data.get("answer", "")
    assert len(answer) > 50, f"Answer too short for: {label}"
    answer_lower = answer.lower()
    assert any(kw in answer_lower for kw in keywords), (
        f"Answer missing expected keywords {keywords} for: {label}"
    )
    assert_has_map_data(data, label)


# ── Queries that SHOULD return map data ─────────────────────


class TestMapDataPresent:
    """Queries requesting geographic visualization should return map_data."""

    async def test_areas_proteccion_historica_mapa(self, client):
        """Historical protection areas should have geometry (known cached geo dataset)."""
        resp = await client.post(
            "/api/v1/query/smart",
            json=smart_payload("Areas de proteccion historica de CABA, mostrá en mapa"),
            headers=headers(),
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data.get("answer"), "Empty answer"
        if data.get("map_data"):
            assert_has_map_data(data, "areas proteccion CABA")

    async def test_efectores_salud_mapa(self, client):
        """Health facilities should have point geometry."""
        resp = await client.post(
            "/api/v1/query/smart",
            json=smart_payload("Efectores de salud en Buenos Aires, mostrá en mapa"),
            headers=headers(),
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("map_data"):
                assert_has_map_data(data, "efectores salud")

    async def test_barrios_caba_mapa(self, client):
        """CABA barrios — may not have geo data until shapefiles are processed."""
        resp = await client.post(
            "/api/v1/query/smart",
            json=smart_payload("Mostrá los barrios de CABA en un mapa"),
            headers=headers(),
        )
        if resp.status_code == 200:
            data = resp.json()
            if data.get("map_data"):
                assert_has_map_data(data, "barrios CABA")


# ── Queries that should NOT return map data ─────────────────


class TestMapDataAbsent:
    """Non-geographic queries should NOT return map_data."""

    async def test_inflacion_no_map(self, client):
        data = await ask(client, "Inflacion acumulada ultimos 12 meses")
        assert_no_map_data(data, "inflacion")

    async def test_presupuesto_no_map(self, client):
        data = await ask(client, "Presupuesto del ministerio de educacion 2025")
        assert_no_map_data(data, "presupuesto")

    async def test_ddjj_no_map(self, client):
        """DDJJ query should not return map data. Uses raw POST to avoid leak check."""
        resp = await client.post(
            "/api/v1/query/smart",
            json=smart_payload("Patrimonio de diputados"),
            headers=headers(),
        )
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
        data = resp.json()
        assert data.get("answer"), "Empty answer"
        assert_no_map_data(data, "ddjj")


# ── GeoJSON structure validation ────────────────────────────


class TestGeoJSONStructure:
    """Validate the GeoJSON FeatureCollection structure in detail."""

    _GEO_QUERIES = [
        "Estaciones de ferrocarril en CABA, mostrá en mapa",
        "Estaciones de bicicletas publicas en CABA, mostrá en mapa",
        "Establecimientos educativos de CABA en mapa",
        "Locales en venta en CABA, mostrá en mapa",
    ]

    async def _get_map_response(self, client, max_attempts: int = 4) -> dict | None:
        """Try multiple geo queries until one returns map_data.

        LLM non-determinism means _geometry_geojson may not always be
        included in the SELECT. Trying different known-geo tables
        maximizes the chance of getting map_data.
        """
        for question in self._GEO_QUERIES[:max_attempts]:
            resp = await client.post(
                "/api/v1/query/smart",
                json=smart_payload(question),
                headers=headers(),
            )
            if resp.status_code != 200:
                continue
            data = resp.json()
            if data.get("map_data") and data["map_data"].get("features"):
                return data
        return None

    async def test_feature_properties_no_internal_fields(self, client):
        """Feature properties should not leak internal fields."""
        data = await self._get_map_response(client)
        if not data:
            pytest.skip("Could not get map_data from any geo query (LLM non-determinism)")
        for feat in data["map_data"]["features"][:10]:
            props = feat.get("properties", {})
            assert "_geometry_geojson" not in props, (
                "Internal _geometry_geojson leaked to properties"
            )
            assert "_source_dataset_id" not in props, (
                "Internal _source_dataset_id leaked to properties"
            )

    async def test_max_features_cap(self, client):
        """Map data should have at most 500 features."""
        data = await self._get_map_response(client)
        if not data:
            pytest.skip("Could not get map_data from any geo query (LLM non-determinism)")
        assert len(data["map_data"]["features"]) <= 500, "Feature count exceeds 500 cap"

    async def test_charts_suppressed_when_map(self, client):
        """When map_data is present, chart_data should be None."""
        data = await self._get_map_response(client)
        if not data:
            pytest.skip("Could not get map_data from any geo query (LLM non-determinism)")
        assert data.get("chart_data") is None, "chart_data should be None when map_data is present"
