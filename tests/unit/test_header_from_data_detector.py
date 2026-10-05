"""Detector `header_from_data`: tablas cuyos nombres de columna son datos.

Los casos son tablas reales medidas en staging el 04-oct-2026 (columnas físicas
y `columns_json` tal cual): las que rompió el colector tienen que dispararlo, y
los pivots legítimos —años, meses, fechas como columnas desde el origen— no.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from app.application.validation.collector_hooks import validate_post_parse
from app.application.validation.detector import Mode, ResourceContext, Severity
from app.application.validation.detectors import build_default_detectors
from app.application.validation.detectors.headers import (
    KIND_DATA,
    KIND_NUMBER,
    KIND_PERIOD,
    HeaderFromDataDetector,
    classify_column_name,
    evaluate_header,
)
from app.infrastructure.celery.tasks import collector_tasks as ct

DATASET_ID = "e5ceaaf8-4e4b-4a62-bf69-a9909b947ced"

LEYES_HEADER = [
    "PROYECTO_ID",
    "CAMARA_SANCIONADORA",
    "SANCION_DEFINITIVA",
    "LEY",
    "EXPEDIENTE_INICIAL",
    "PRIMERA_MEDIA_SANCION",
    "SEGUNDA_MEDIA_SANCION",
]
# diputados__leyes_sancionadas__11b91158__v2 (CSV, rota).
LEYES_ROTA = [
    "HCDN285290 / HCDN287440",
    "Senado",
    "2025-12-26T00:00:00",
    "2025-12-26T00:00:00_2",
    "0003-PE-2025 / 0014-JGM-2025",
    "2025-12-17T00:00:00",
    "2025-12-17T00:00:00_2",
]
# diputados__leyes_sancionadas__7ce083ad__v1 (JSON, gemela sana).
LEYES_SANA = [
    *LEYES_HEADER,
    "_source_dataset_id",
    "_source_url",
    "_source_file_hash",
    "_parser_version",
    "_collector_version",
]


@pytest.mark.parametrize(
    ("name", "kind"),
    [
        ("2025-12-26T00:00:00", KIND_DATA),
        ("16c52775-f59e-4bba-8b81-a346977b33f7", KIND_DATA),
        ("https://www.indec.gob.ar/ftp/cuadros/soc", KIND_DATA),
        ("HCDN285290 / HCDN287440", KIND_DATA),
        ("0003-PE-2025 / 0014-JGM-2025", KIND_DATA),
        ("prensa@arzbaires.org.ar", KIND_DATA),
        ("08FEB1973:00:00:00", KIND_DATA),
        ("0.46", KIND_NUMBER),
        ("3812.0 / DEMOCRATA CRISTIANO", KIND_NUMBER),
        ("1.234.567", KIND_NUMBER),
        # Pivots legítimos.
        ("2019", KIND_PERIOD),
        ("2020.0", KIND_PERIOD),
        ("2016.1", KIND_PERIOD),  # el segundo `2016` que pandas desambigua
        ("2015-11-01 00:00:00", KIND_PERIOD),
        ("Enero 2024", KIND_PERIOD),
        ("2do. semestre 2016", KIND_PERIOD),
        # Encabezados comunes.
        ("PROYECTO_ID", None),
        ("Nombre_Bloque", None),
        ("1.1", None),  # código de pregunta en las encuestas del INDEC
        ("col_3", None),
    ],
)
def test_clasificacion_de_nombres(name, kind):
    assert classify_column_name(name) == kind


def test_dispara_con_la_tabla_que_rompio_el_colector():
    verdict = evaluate_header([*LEYES_ROTA, "_source_dataset_id"], None)

    assert verdict is not None
    assert verdict.rule == "data_in_header"
    assert "HCDN285290 / HCDN287440" in verdict.data_names


def test_no_dispara_con_la_gemela_sana():
    assert evaluate_header(LEYES_SANA, json.dumps(LEYES_SANA)) is None
    assert evaluate_header(LEYES_SANA, None) is None


def test_renombrada_contra_lo_declarado_neuquen_bloques():
    """Un solo nombre de datos (el UUID de `_source_dataset_id` renombrado)
    alcanza cuando la tabla difiere del encabezado declarado, que era limpio."""
    physical = [
        "Arriba",
        "Stillger Gisselle Janette",
        "Presidente",
        "16c52775-f59e-4bba-8b81-a346977b33f7",
    ]
    declared = ["Nombre_Bloque", "Nombre_Diputado", "Cargo", "_source_dataset_id"]

    verdict = evaluate_header(physical, declared)

    assert verdict is not None
    assert verdict.rule == "renamed_from_declared"
    assert verdict.data_names == ["16c52775-f59e-4bba-8b81-a346977b33f7"]


def test_renombrada_contra_lo_declarado_neuquen_comisiones():
    physical = [
        "A",
        "Legislacion de Asuntos Constitucionales y Justicia",
        "Novoa",
        "Ernesto",
        "2023-12-10T00:00:00",
        "2027-12-10T00:00:00",
        "PRESIDENTE",
    ]
    declared = [
        "Comision_Letra",
        "Comision_Nombre",
        "Diputado_Apellido",
        "Diputado_Nombre",
        "Fecha_Ingreso_Diputados_en_Comision",
        "Fecha_Cese_Diputados_en_Comision",
        "Cargo",
        "_source_dataset_id",
    ]

    verdict = evaluate_header(physical, declared)

    assert verdict is not None and verdict.rule == "renamed_from_declared"


def test_numeros_como_nombres_disparan_si_son_la_mitad_de_las_columnas():
    # datos_gob_ar__canon_hidrocarburifero (Excel): la fila de datos quedó de encabezado.
    canon = ["CANON DE EXPLORACIÓN", "Primer Período", "0.46", "29.15693189994341", "_source_url"]
    assert evaluate_header(canon, None) is not None
    # Entre Ríos: resultados electorales con la fila de votos de encabezado.
    entre_rios = [
        "JUSTICIALISTA / DEMOCRATA CRISTIANO",
        "3812.0 / DEMOCRATA CRISTIANO",
        "51.65 / DEMOCRATA CRISTIANO",
    ]
    assert evaluate_header(entre_rios, None) is not None


@pytest.mark.parametrize(
    "columns",
    [
        # Córdoba: pivot mensual con fechas de Excel como columnas.
        ["Concepto", "2015-11-01 00:00:00", "2015-12-01 00:00:00", "2016-01-01 00:00:00"],
        # Cuadro anual.
        ["Provincia", "2019", "2020", "2021", "2022"],
        # INDEC pobreza histórica: semestres como columnas.
        [
            "Region",
            "Primer semestre de 2003",
            "Segundo semestre de 2003",
            "Primer semestre de 2004",
        ],
        # Mendoza deuda: años repetidos que pandas desambigua.
        ["acreedor", "id_acreedor", "porcentaje", "moneda", "garantia", "2016", "2016.1", "2016.2"],
        # ENGHo: microdatos con códigos de pregunta.
        ["id", "provincia", "region", "pondera", "1.1", "1.2", "1.3", "2.1", "2.2", "3.1"],
        # Un número suelto en una tabla ancha no alcanza.
        ["acreedor", "id_acreedor", "saldo", "porcentaje", "moneda", "garantia", "1.732", "186.19"],
    ],
)
def test_no_dispara_con_pivots_ni_codigos_legitimos(columns):
    assert evaluate_header(columns, json.dumps(columns)) is None
    assert evaluate_header(columns, None) is None


def test_una_promocion_legitima_no_dispara():
    """Excel con `Unnamed: N` declarado y un encabezado real escrito: lo físico
    difiere de lo declarado, pero lo declarado no era limpio y los nombres
    nuevos no son datos."""
    declared = ["Cuadro 1", "Unnamed: 1", "Unnamed: 2", "Unnamed: 3"]
    physical = ["Provincia", "2019", "2020", "2021"]
    assert evaluate_header(physical, declared) is None


def test_el_detector_en_post_parse_lee_las_columnas_fisicas():
    detector = HeaderFromDataDetector()
    ctx = ResourceContext(
        resource_id="d1",
        materialized_columns=[*LEYES_HEADER, "_source_dataset_id"],
        columns_json=json.dumps([*LEYES_HEADER, "_source_dataset_id"]),
        metadata={"physical_columns": [*LEYES_ROTA, "_source_dataset_id"]},
    )

    finding = detector.run(ctx, Mode.POST_PARSE)

    assert finding is not None
    assert finding.severity == Severity.CRITICAL
    assert finding.payload["rule"] == "renamed_from_declared"
    assert finding.should_redownload is False


def test_el_detector_retrospectivo_usa_las_materializadas():
    detector = HeaderFromDataDetector()
    ctx = ResourceContext(resource_id="d1", materialized_columns=LEYES_ROTA, columns_json="[]")
    finding = detector.run(ctx, Mode.RETROSPECTIVE)
    assert finding is not None and finding.payload["rule"] == "data_in_header"


def test_esta_en_la_suite_por_defecto_y_la_severidad_se_puede_bajar(monkeypatch):
    monkeypatch.delenv("OPENARG_HEADER_FROM_DATA_SEVERITY", raising=False)
    names = {d.name: d for d in build_default_detectors()}
    assert names["header_from_data"].severity == Severity.CRITICAL

    monkeypatch.setenv("OPENARG_HEADER_FROM_DATA_SEVERITY", "warn")
    names = {d.name: d for d in build_default_detectors()}
    assert names["header_from_data"].severity == Severity.WARN


def test_el_gate_post_parse_rechaza_la_tabla_rota(monkeypatch):
    """De punta a punta por el hook real: con las columnas físicas en
    `metadata`, la primera crítica es `header_from_data`."""
    import app.application.validation.collector_hooks as hooks

    monkeypatch.delenv("OPENARG_DISABLE_INGESTION_VALIDATOR", raising=False)
    monkeypatch.delenv("OPENARG_HEADER_FROM_DATA_SEVERITY", raising=False)
    monkeypatch.setattr(hooks, "_validator_singleton", None)
    monkeypatch.setattr(hooks, "_persist", lambda engine, ctx, findings: None)

    finding = validate_post_parse(
        MagicMock(),
        dataset_id="d1",
        portal="diputados",
        declared_format="csv",
        table_name="raw.diputados__leyes_sancionadas__11b91158__v2",
        materialized_columns=[*LEYES_HEADER, "_source_dataset_id"],
        materialized_row_count=1319,
        declared_size_bytes=100_000,
        columns_json=json.dumps([*LEYES_HEADER, "_source_dataset_id"]),
        metadata={"physical_columns": [*LEYES_ROTA, "_source_dataset_id"]},
    )

    assert finding is not None
    assert finding.detector_name == "header_from_data"


# ── el gate post-parse ve la tabla, no lo que el parser dijo ───────────────


def test_el_gate_post_parse_recibe_las_columnas_fisicas():
    physical = ["HCDN285290 / HCDN287440", "Senado", "2025-12-26T00:00:00", "_source_dataset_id"]
    captured: dict = {}

    def _capture(engine, **kwargs):
        captured.update(kwargs)
        return None

    with (
        patch.object(ct, "_existing_columns", return_value=physical) as existing,
        patch.object(ct, "_ws0_validate_post_parse", side_effect=_capture),
        patch.object(ct, "_apply_cached_outcome"),
    ):
        ct._finalize_cached_dataset(
            MagicMock(),
            dataset_id=DATASET_ID,
            portal="diputados",
            source_id="leyes",
            table_name="raw.diputados__leyes_sancionadas__11b91158__v2",
            row_count=1319,
            columns=[*LEYES_HEADER, "_source_dataset_id"],
            declared_format="csv",
        )

    existing.assert_called_once_with(
        existing.call_args.args[0], "raw", "diputados__leyes_sancionadas__11b91158__v2"
    )
    assert captured["metadata"] == {"physical_columns": physical}
    assert captured["materialized_columns"] == [*LEYES_HEADER, "_source_dataset_id"]
