"""El colector no reemplaza encabezados buenos ni trunca tablas por chunk.

Medido el 04-oct-2026 (plan-arreglos-openarg-verificado, ítem 4.2 y "lo que no
vio" 5): `_to_sql_safe` volvía a inferir el encabezado en cada escritura y en
cada chunk. Con encabezados buenos promovía filas de datos (`leyes_sancionadas`
CSV quedó con columnas `'HCDN285290 / HCDN287440'`, `'2025-12-26T00:00:00'`),
renombraba `_source_dataset_id` a su UUID, y en CSV por chunks cada chunk salía
con columnas distintas: el append fallaba, la tabla se recreaba y quedaba sólo
el último chunk (`proyectos_parlamentarios`: 11.089 filas de 111.091).

Los frames de estos tests son las primeras filas reales de esas tablas, leídas
de staging en sólo lectura (la gemela JSON de leyes, que está bien, y las
tablas de la Legislatura de Neuquén reconstruidas con su `columns_json`).
"""

from __future__ import annotations

import io
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from app.application.pipeline.parsers.header_inference import (
    HeaderMismatchError,
    apply_header_decision,
    decide_header,
)
from app.infrastructure.celery.tasks import collector_tasks as ct

DATASET_ID = "e5ceaaf8-4e4b-4a62-bf69-a9909b947ced"

# Primeras líneas reales de leyes_sancionadas3.2.csv (HCDN), en el orden del archivo.
LEYES_CSV = """PROYECTO_ID,CAMARA_SANCIONADORA,SANCION_DEFINITIVA,LEY,EXPEDIENTE_INICIAL,PRIMERA_MEDIA_SANCION,SEGUNDA_MEDIA_SANCION
HCDN285290,Senado,2025-12-26T00:00:00,27799,0003-PE-2025,2025-12-17T00:00:00,
HCDN287440,Senado,2025-12-26T00:00:00,27798,0014-JGM-2025,2025-12-17T00:00:00,
HCDN261442,Senado,2025-09-18T00:00:00,27797,3805-D-2022,2023-12-07T00:00:00,
HCDN285061,Senado,2025-08-21T00:00:00,27796,2789-D-2025,2025-08-06T00:00:00,
HCDN282849,Senado,2025-08-21T00:00:00,27795,0839-D-2025,2025-08-06T00:00:00,
HCDN286131,Diputados,2025-08-20T00:00:00,27794,0021-S-2025,2025-07-10T00:00:00,
HCDN280905,Senado,2025-07-10T00:00:00,27793,7861-D-2024,2025-06-04T00:00:00,
HCDN279460,Senado,2025-07-10T00:00:00,27792,6691-D-2024,2025-06-04T00:00:00,
HCDN282731,Senado,2025-07-10T00:00:00,27791,0684-D-2025,2025-06-04T00:00:00,
HCDN282431,Diputados,2025-06-04T00:00:00,27790,0007-S-2025,2025-05-07T00:00:00,
HCDN271832,Senado,2025-05-07T00:00:00,27789,0024-PE-2023,2024-10-01T00:00:00,
"""
LEYES_HEADER = [
    "PROYECTO_ID",
    "CAMARA_SANCIONADORA",
    "SANCION_DEFINITIVA",
    "LEY",
    "EXPEDIENTE_INICIAL",
    "PRIMERA_MEDIA_SANCION",
    "SEGUNDA_MEDIA_SANCION",
]

# neuquen_legislatura__bloques__c6fe917c__v1 (rota el 04-oct): columns_json
# [Nombre_Bloque, Nombre_Diputado, Cargo]; la primera fila del archivo quedó
# como encabezado y `_source_dataset_id` pasó a llamarse como su UUID.
BLOQUES_CSV = """Nombre_Bloque,Nombre_Diputado,Cargo
Arriba,Stillger Gisselle Janette,Presidente
Avanzar,Lepore Francisco,Presidente
Comunidad,Novoa Ernesto,Presidente
Comunidad,Hermosilla Yamila Abigail,
Comunidad,Martínez Matías Nicolás,
Comunidad,Reina Zulma Graciela,
Comunidad,Rios Luz Ailín,
Cumplir,Buchiniz Zaniuk Brenda,
Democracia Neuquén,Méndez Juan Federico,Presidente
"""
BLOQUES_DATASET_ID = "16c52775-f59e-4bba-8b81-a346977b33f7"

# neuquen_legislatura__diputados_en_comision__4c6586eb__v1 (una tabla nueva por día).
COMISION_CSV = """Comision_Letra,Comision_Nombre,Diputado_Apellido,Diputado_Nombre,Fecha_Ingreso_Diputados_en_Comision,Fecha_Cese_Diputados_en_Comision,Cargo
A,Legislacion de Asuntos Constitucionales y Justicia,Novoa,Ernesto,2023-12-10T00:00:00,2027-12-10T00:00:00,PRESIDENTE
A,Legislacion de Asuntos Constitucionales y Justicia,Coggiola,Carlos Alberto,2023-12-10T00:00:00,2027-12-10T00:00:00,SECRETARIO
A,Legislacion de Asuntos Constitucionales y Justicia,alamo,Gabriel Marcial,2023-12-10T00:00:00,2027-12-10T00:00:00,VOCAL
A,Legislacion de Asuntos Constitucionales y Justicia,Canuto,Damian,2023-12-10T00:00:00,2027-12-10T00:00:00,VOCAL
"""

PROYECTOS_HEADER = (
    "PROYECTO_ID,TITULO,PUBLICACION_FECHA,PUBLICACION_ID,CAMARA_ORIGEN,"
    "EXP_DIPUTADOS,EXP_SENADO,TIPO,AUTOR"
)


def _proyectos_csv(n_rows: int) -> str:
    """Filas con la forma de proyectos_parlamentarios: EXP_SENADO vacío."""
    lines = [PROYECTOS_HEADER]
    for i in range(n_rows):
        lines.append(
            f"HCDN{289000 + i},PROYECTO DE LEY NUMERO {i} SOBRE TEMA {i % 7},"
            f'2026-03-05T00:00:00,HCDN144TP006,Diputados,{i:04d}-D-2026,,LEY,"TODERO, PABLO"'
        )
    return "\n".join(lines) + "\n"


def _read(csv_text: str) -> pd.DataFrame:
    return pd.read_csv(io.StringIO(csv_text))


# ── encabezados buenos ─────────────────────────────────────────────────────


def test_un_encabezado_bueno_no_se_reemplaza_por_filas_de_datos():
    """leyes_sancionadas: la vía `sparse` combinaba las dos primeras filas."""
    df = _read(LEYES_CSV).assign(_source_dataset_id=DATASET_ID)

    out = ct._sanitize_columns(df)

    assert list(out.columns) == [*LEYES_HEADER, "_source_dataset_id"]
    assert len(out) == 11
    assert out.iloc[0]["PROYECTO_ID"] == "HCDN285290"


def test_sanear_dos_veces_da_lo_mismo():
    df = _read(LEYES_CSV).assign(_source_dataset_id=DATASET_ID)

    once = ct._sanitize_columns(df)
    twice = ct._sanitize_columns(once.copy())

    assert list(twice.columns) == list(once.columns)
    assert len(twice) == len(once) == 11


def test_un_empate_contra_el_encabezado_no_es_una_mejora():
    """Neuquén `bloques`: la fila `Arriba, Stillger…, Presidente` empataba con el
    encabezado (0 placeholders y el mismo alpha de cada lado) y ganaba."""
    df = _read(BLOQUES_CSV).assign(_source_dataset_id=BLOQUES_DATASET_ID)

    out = ct._sanitize_columns(df)

    assert list(out.columns) == ["Nombre_Bloque", "Nombre_Diputado", "Cargo", "_source_dataset_id"]
    assert len(out) == 9
    assert out.iloc[0]["Nombre_Bloque"] == "Arriba"
    assert BLOQUES_DATASET_ID not in out.columns


def test_fechas_iso_con_hora_no_son_texto_de_encabezado():
    df = _read(COMISION_CSV).assign(_source_dataset_id=DATASET_ID)

    out = ct._sanitize_columns(df)

    assert list(out.columns)[:4] == [
        "Comision_Letra",
        "Comision_Nombre",
        "Diputado_Apellido",
        "Diputado_Nombre",
    ]
    assert len(out) == 4


def test_la_ruta_de_escritura_no_vuelve_a_inferir_el_encabezado():
    """`_to_sql_safe` escribe las columnas que recibe: no mira filas."""
    df = _read(LEYES_CSV).assign(_source_dataset_id=DATASET_ID)
    conn = MagicMock()
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    conn.in_transaction.return_value = False
    written: list[pd.DataFrame] = []

    def _capture(self, *args, **kwargs):
        written.append(self.copy())

    with patch.object(pd.DataFrame, "to_sql", _capture):
        ct._to_sql_safe(df, "cache_test", engine, if_exists="replace", index=False)

    assert list(written[0].columns) == [*LEYES_HEADER, "_source_dataset_id"]
    assert len(written[0]) == 11


# ── columnas de linaje ─────────────────────────────────────────────────────


def test_las_columnas_de_linaje_no_participan_ni_se_renombran():
    """Una promoción legítima (encabezado de dos filas sobre `Unnamed: N`) no
    puede tocar `_source_*`: antes salían renombradas con el UUID y la URL, y
    las que venían en None se rellenaban hacia adelante con la URL de al lado."""
    df = pd.DataFrame(
        [
            ["Provincia", None, "Departamento", None],
            ["Nombre", "Codigo", "Nombre", "Codigo"],
            ["Buenos Aires", "06", "La Plata", "441"],
        ],
        columns=["Unnamed: 0", "Unnamed: 1", "Unnamed: 2", "Unnamed: 3"],
    )
    df["_source_dataset_id"] = DATASET_ID
    df["_source_url"] = "https://www.indec.gob.ar/ftp/cuadros/sociedad/cuadros_pobreza.xls"
    df["_source_file_hash"] = None
    df["_parser_version"] = None
    df["_collector_version"] = None

    out = ct._sanitize_columns(df)

    assert list(out.columns) == [
        "Provincia / Nombre",
        "Provincia / Codigo",
        "Departamento / Nombre",
        "Departamento / Codigo",
        "_source_dataset_id",
        "_source_url",
        "_source_file_hash",
        "_parser_version",
        "_collector_version",
    ]
    assert len(out) == 1
    assert out.iloc[0]["_source_url"].startswith("https://www.indec.gob.ar/")


# ── una decisión por archivo ───────────────────────────────────────────────


def test_la_decision_del_primer_chunk_se_aplica_igual_a_los_siguientes():
    first = pd.DataFrame(
        [["Solicitud", "Provincia", "Departamento"], ["A", "Buenos Aires", "La Plata"]],
        columns=["Unnamed: 0", "Unnamed: 1", "Unnamed: 2"],
    )
    later = pd.DataFrame(
        [["B", "Córdoba", "Capital"], ["Solicitud", "Provincia", "Departamento"]],
        columns=["Unnamed: 0", "Unnamed: 1", "Unnamed: 2"],
    )

    decided, decision = decide_header(first)
    applied = apply_header_decision(later, decision)

    assert list(decided.columns) == ["Solicitud", "Provincia", "Departamento"]
    assert decision.rows_consumed == 1
    # Ni se re-infiere ni se consumen filas: la fila que "parece" encabezado
    # en el medio del archivo es un dato.
    assert list(applied.columns) == ["Solicitud", "Provincia", "Departamento"]
    assert len(applied) == 2


def test_un_chunk_con_otras_columnas_no_se_escribe_corrido():
    _decided, decision = decide_header(_read(LEYES_CSV))
    other = pd.DataFrame({"x": [1], "y": [2]})

    with pytest.raises(HeaderMismatchError):
        apply_header_decision(other, decision)


def test_csv_por_chunks_decide_una_vez_y_ningun_chunk_recrea_la_tabla(tmp_path):
    csv_path = tmp_path / "proyectos.csv"
    csv_path.write_text(_proyectos_csv(12), encoding="utf-8")
    calls: list[dict] = []

    def _fake_to_sql_safe(df, table_name, engine, **kwargs):
        calls.append({"columns": list(df.columns), "rows": len(df), **kwargs})

    with patch.object(ct, "_to_sql_safe", side_effect=_fake_to_sql_safe):
        total, columns, truncated = ct._csv_load_inner(
            str(csv_path),
            "cache_proyectos",
            MagicMock(),
            {"sep": ","},
            4,
            source_dataset_id=DATASET_ID,
        )

    expected = [*PROYECTOS_HEADER.split(","), "_source_dataset_id"]
    assert total == 12 and truncated is False
    assert columns == expected
    assert [c["rows"] for c in calls] == [4, 4, 4]
    assert all(c["columns"] == expected for c in calls)
    assert calls[0]["if_exists"] == "replace"
    assert calls[0].get("allow_recreate", True) is True
    assert all(c["if_exists"] == "append" and c["allow_recreate"] is False for c in calls[1:])


def test_un_chunk_que_no_entra_no_dropea_la_tabla():
    df = pd.DataFrame({"a": [1]})
    conn = MagicMock()
    engine = MagicMock()
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    conn.in_transaction.return_value = False

    error = RuntimeError('column "a" of relation "cache_test" does not exist')
    with (
        patch.object(pd.DataFrame, "to_sql", side_effect=error),
        patch.object(ct, "_record_cache_drop") as record_drop,
        pytest.raises(ct._ChunkAppendError),
    ):
        ct._to_sql_safe(
            df, "cache_test", engine, if_exists="append", index=False, allow_recreate=False
        )

    record_drop.assert_not_called()
    begin_sql = [
        str(c.args[0])
        for c in engine.begin.return_value.__enter__.return_value.execute.call_args_list
    ]
    assert not any("DROP TABLE" in s for s in begin_sql)


def test_si_un_chunk_no_entra_se_recarga_el_archivo_entero_como_texto():
    with patch.object(
        ct,
        "_csv_load_inner",
        side_effect=[ct._ChunkAppendError("dtype drift"), (30, ["a", "b"], False)],
    ) as inner:
        result = ct._load_csv_chunked(
            "/tmp/fake.csv",
            "cache_test",
            MagicMock(),
            chunk_size=10,
            csv_params_override={"sep": ","},
        )

    assert result == (30, ["a", "b"], False)
    assert inner.call_count == 2
    assert "dtype" not in inner.call_args_list[0].args[3]
    assert inner.call_args_list[1].args[3]["dtype"] is str


def test_con_force_append_no_se_recarga_para_no_duplicar():
    with (
        patch.object(ct, "_csv_load_inner", side_effect=ct._ChunkAppendError("x")),
        pytest.raises(ct._ChunkAppendError),
    ):
        ct._load_csv_chunked(
            "/tmp/fake.csv",
            "cache_test",
            MagicMock(),
            chunk_size=10,
            force_append=True,
            csv_params_override={"sep": ","},
        )


def test_tipos_de_un_chunk_que_la_tabla_corromperia():
    first_kinds = {"entero": "i", "real": "f", "texto": "O"}
    chunk = pd.DataFrame(
        {
            "entero": [1.0, 2.5],  # Postgres redondearía 2.5 en silencio
            "real": [1.5, None],
            "texto": [1, 2],
        }
    )
    assert ct._chunk_dtype_conflicts(first_kinds, chunk) == ["entero"]

    nan_ints = pd.DataFrame({"entero": [1.0, None], "real": ["x", None], "texto": ["a", "b"]})
    assert ct._chunk_dtype_conflicts(first_kinds, nan_ints) == ["real"]


def test_la_version_del_parser_cubre_la_inferencia_de_encabezado():
    """Sin esto, el arreglo no movía la versión registrada en `raw_table_versions`
    y G1 le atribuía al portal un cambio de forma que era nuestro."""
    from app.application.catalog.parser_fingerprint import _PARSER_MODULES

    assert "app.application.pipeline.parsers.header_inference" in _PARSER_MODULES
    assert "app.application.pipeline.parsers.header_tokens" in _PARSER_MODULES
