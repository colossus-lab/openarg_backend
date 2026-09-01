"""Nadie puede levantar una cuarentena sin decirlo.

`catalog_resources.materialization_status` lo escriben varios lugares con
intenciones opuestas: la escalera de reparación lo pone en
`materialization_corrupted` para sacar del servicio una tabla ilegible, y varias
rutas de ingesta lo devuelven a `ready` como parte de su trabajo normal.

Esto se encontró dos veces por accidente. La primera fueron `_db.py` y
`collector_tasks.py`. La segunda, medida en producción el 2026-09-01, fue
`catalog_backfill`: reescribe las 32.706 filas **cada 30 minutos**, así que las
tablas que los avisos de esa madrugada decían haber retirado estaban todas en
`ready` — la cuarentena duraba menos de media hora y nadie lo veía.

Por eso este test no revisa escritores conocidos: **los enumera**. Un cuarto
escritor que aparezca mañana falla acá en vez de descubrirse en producción.
"""

from __future__ import annotations

import re
from pathlib import Path

_SRC = Path("src/app")

# El módulo que ES la cuarentena: escribe el estado a propósito y no se audita
# a sí mismo.
_DUENO = "application/repair/quarantine.py"

# Escribir el estado sin leerlo desde EXCLUDED —p. ej. marcarlo `failed` en una
# ruta de error— no pisa una cuarentena, porque no viene de un upsert.
_PATRON_UPSERT = re.compile(
    r"materialization_status\s*=\s*EXCLUDED\.materialization_status", re.IGNORECASE
)
_PATRON_READY = re.compile(r"materialization_status\s*=\s*'ready'", re.IGNORECASE)


def _archivos_que_escriben_el_estado() -> list[Path]:
    encontrados = []
    for f in _SRC.rglob("*.py"):
        if f.as_posix().endswith(_DUENO):
            continue
        texto = f.read_text(encoding="utf-8")
        if _PATRON_UPSERT.search(texto) or _PATRON_READY.search(texto):
            encontrados.append(f)
    return encontrados


def test_todo_el_que_escribe_el_estado_respeta_la_cuarentena():
    sin_guarda = [
        f.relative_to(_SRC).as_posix()
        for f in _archivos_que_escriben_el_estado()
        if "materialization_corrupted" not in f.read_text(encoding="utf-8")
    ]
    assert not sin_guarda, (
        "estos archivos escriben materialization_status y no excluyen "
        f"'materialization_corrupted', así que levantan la cuarentena en silencio: {sin_guarda}"
    )


def test_el_enumerador_encuentra_algo():
    # Si el patrón deja de matchear —porque alguien reescribe la SQL de otra
    # forma— el test de arriba pasaría vacío y no protegería nada.
    assert _archivos_que_escriben_el_estado(), "el enumerador no encontró ningún escritor"


def test_los_dos_upserts_del_backfill_estan_cubiertos():
    # El caso concreto que se escapó: dos upserts en el mismo archivo.
    texto = (_SRC / "infrastructure/celery/tasks/catalog_backfill.py").read_text(encoding="utf-8")
    assert texto.count("materialization_corrupted") == 2, (
        "el backfill tiene DOS upserts sobre catalog_resources; los dos necesitan la guarda"
    )


def test_la_guarda_conserva_el_valor_existente_no_lo_borra():
    # `CASE ... THEN catalog_resources.materialization_status` conserva; un
    # `THEN NULL` o un DO NOTHING cambiarían el significado.
    texto = (_SRC / "infrastructure/celery/tasks/catalog_backfill.py").read_text(encoding="utf-8")
    assert "THEN catalog_resources.materialization_status" in texto
