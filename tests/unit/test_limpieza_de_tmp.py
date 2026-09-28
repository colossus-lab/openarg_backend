"""Un solo barrido de `/tmp`, y que no borre fuera de su directorio.

Hasta el 2026-09-28 había **dos** tareas haciendo lo mismo sobre el mismo
directorio: `openarg.ops_temp_dir_cleanup` (horaria, en `ops_fixes`) y
`openarg.cleanup_orphan_temp_files` (cada 30 min, en `collector_tasks`).
Las dos barrían `tmp*` viejos del temp dir del worker que las ejecutaba.

Sobrevive `cleanup_orphan_temp_files` porque estaba mejor integrada —
agendada en las tres colas de colector (PR #61) y disparada además por
`worker_process_init` al arrancar cada worker— pero se quedó con lo mejor
de la otra:

1. **La guarda de contención.** Un `tmp*` puede ser un symlink a cualquier
   parte del disco, y esto corre como root dentro del contenedor. Ahora se
   compara por `realpath` contra el temp dir antes de borrar nada.
2. **El tamaño recursivo.** Un directorio reporta el tamaño de su entrada,
   no el de su contenido: al borrar 4 GB de restos de extracción, la
   métrica que existe para seguir el leak informaba unos pocos KiB.

El leak que motiva todo esto llegó a 89 GB en staging y llenó el disco
de la EC2.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from app.infrastructure.celery.tasks.collector_tasks import cleanup_orphan_temp_files


def _envejecer(path: Path, *, segundos: int) -> None:
    """Retrasar el mtime de `path`, del enlace y no de su destino.

    `os.utime` sigue los symlinks por defecto: sin `follow_symlinks=False`
    esto envejece el archivo apuntado y deja el enlace con fecha de hoy, así
    que el barrido lo saltea por reciente y el test falla por el motivo
    equivocado.
    """
    ts = time.time() - segundos
    os.utime(path, (ts, ts), follow_symlinks=not path.is_symlink())


@pytest.fixture
def temp_dir(tmp_path, monkeypatch):
    monkeypatch.setenv("OPENARG_TEMP_DIR", str(tmp_path))
    return tmp_path


def test_borra_un_directorio_viejo_no_vacio(temp_dir):
    """El caso real: restos de una extracción interrumpida."""
    viejo = temp_dir / "tmpabc123"
    viejo.mkdir()
    (viejo / "payload.bin").write_bytes(b"x" * 32)
    _envejecer(viejo / "payload.bin", segundos=7200)
    _envejecer(viejo, segundos=7200)

    r = cleanup_orphan_temp_files.run(3600)

    assert not viejo.exists()
    assert r["reaped"] == 1
    assert r["bytes_freed"] >= 32, "un directorio tiene que informar lo que tenía adentro"


def test_no_toca_lo_reciente(temp_dir):
    """Una descarga en curso no se puede borrar."""
    reciente = temp_dir / "tmpreciente"
    reciente.mkdir()
    (reciente / "payload.bin").write_bytes(b"x" * 16)

    r = cleanup_orphan_temp_files.run(3600)

    assert reciente.exists()
    assert r["reaped"] == 0
    assert r["skipped"] >= 1


def test_un_symlink_viejo_no_arrastra_su_destino(temp_dir, tmp_path_factory):
    """La razón de ser de la guarda de contención.

    Borra el enlace, nunca lo que está del otro lado: esto corre como root
    y un `tmp*` puede apuntar a cualquier parte del disco.
    """
    afuera = tmp_path_factory.mktemp("afuera")
    victima = afuera / "no_tocar.bin"
    victima.write_bytes(b"x" * 64)

    enlace = temp_dir / "tmpenlace"
    enlace.symlink_to(victima)
    _envejecer(enlace, segundos=7200)

    cleanup_orphan_temp_files.run(3600)

    assert not enlace.exists(), "el enlace viejo sí se borra"
    assert victima.exists(), "el destino del enlace NO se toca"
    assert victima.read_bytes() == b"x" * 64


def test_solo_barre_el_glob_tmp(temp_dir):
    """Cualquier otra cosa en el directorio queda como está."""
    ajeno = temp_dir / "importante.db"
    ajeno.write_bytes(b"x" * 8)
    _envejecer(ajeno, segundos=7200)

    r = cleanup_orphan_temp_files.run(3600)

    assert ajeno.exists()
    assert r["reaped"] == 0


def test_la_tarea_duplicada_ya_no_existe():
    """Eran dos tareas para el mismo trabajo; quedó una.

    Si vuelve a aparecer, que sea una decisión y no un descuido.
    """
    from app.infrastructure.celery.app import celery_app

    agendadas = {e["task"] for e in (celery_app.conf.beat_schedule or {}).values()}
    assert "openarg.ops_temp_dir_cleanup" not in agendadas
    assert "openarg.ops_temp_dir_cleanup" not in (celery_app.conf.task_routes or {})
    assert "openarg.cleanup_orphan_temp_files" in agendadas
