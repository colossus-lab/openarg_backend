"""La tarea que destraba descargas tiene que poder terminar.

`openarg.recover_stuck_tasks` estuvo años sin ejecutarse porque se
despachaba a una cola muerta (ver `test_celery_queues_have_consumers`).
El 2026-09-09, apenas se la ruteó a `ingest` y corrió por primera vez,
murió a los 60 segundos exactos:

    Task openarg.recover_stuck_tasks raised unexpected: OperationalError(
    '(psycopg.OperationalError) sending query failed: another command is
    already in progress')

El `OperationalError` era el síntoma. La causa está más abajo en el mismo
traceback: `SoftTimeLimitExceeded`. Celery abortó la tarea en medio de una
query y el rollback sobre esa conexión rota tiró el error visible.

Por qué no terminaba: el paso 3 preguntaba a `information_schema` **una
vez por cada fila `ready`**. Medido en staging:

    filas `ready`            30.921
    por fila (N+1)           2,4 ms  ->  1,2 min
    el mismo anti-join       0,48 s
    soft_time_limit             60 s

O sea que el paso 3 solo ya excedía el presupuesto, sin contar los pasos
1 y 2. Dos bugs en capas: la cola muerta escondía que además reventaba.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from app.infrastructure.celery.tasks.collector_tasks import recover_stuck_tasks

# La guarda se importa dentro de cada test y no acá arriba a propósito: si
# faltara, un ImportError a nivel de módulo voltearía TODOS los tests de este
# archivo —incluidos los estructurales, que no la usan— y el rojo dejaría de
# decir qué se rompió. Ya me pasó una vez en esta misma sesión.

_FUENTE = Path("src/app/infrastructure/celery/tasks/collector_tasks.py").read_text(encoding="utf-8")


def _cuerpo_paso_3() -> str:
    i = _FUENTE.index("# 3. Validate 'ready' datasets")
    j = _FUENTE.index("if recovered_downloads or recovered_queries or orphaned_ready:", i)
    return _FUENTE[i:j]


# ── que pueda terminar ─────────────────────────────────────


def test_las_filas_ready_se_filtran_en_una_sola_query() -> None:
    """El anti-join lo resuelve el servidor; no se traen las 30.921 filas."""
    cuerpo = _cuerpo_paso_3()

    assert "NOT EXISTS" in cuerpo, (
        "el paso 3 tiene que pedirle a Postgres las filas sin tabla, no "
        "traerse todas las `ready` y preguntar de a una"
    )
    # El bucle sólo puede recorrer candidatos, nunca el universo de `ready`.
    assert "for row in candidatos:" in cuerpo
    assert not re.search(r"for row in ready_datasets", cuerpo)


def test_el_presupuesto_de_tiempo_alcanza_para_el_trabajo() -> None:
    """La cota que hace la diferencia, escrita como número.

    Con una query por fila, 30.921 filas a 2,4 ms son ~74 s contra un
    `soft_time_limit` de 60: imposible por diseño, no por lentitud del día.
    """
    assert recover_stuck_tasks.soft_time_limit == 60
    filas_medidas, ms_por_fila = 30_921, 2.4
    assert filas_medidas * ms_por_fila / 1000 > recover_stuck_tasks.soft_time_limit


# ── la guarda de radio de explosión ────────────────────────


def test_sin_candidatos_no_se_dispara() -> None:
    from app.infrastructure.celery.tasks.collector_tasks import _orphan_guard_tripped

    assert _orphan_guard_tripped(0, 30_921) is False


def test_un_puñado_de_huerfanas_se_procesa() -> None:
    """El caso para el que la tarea existe: unas pocas tablas perdidas."""
    from app.infrastructure.celery.tasks.collector_tasks import (
        _ORPHAN_READY_ABS_GUARD,
        _orphan_guard_tripped,
    )

    assert _orphan_guard_tripped(3, 30_921) is False
    assert _orphan_guard_tripped(_ORPHAN_READY_ABS_GUARD, 30_921) is False


def test_un_catalogo_entero_de_huerfanas_no_se_toca() -> None:
    """Si "desaparecieron" todas, lo que se rompió es el matcheo de nombres.

    Es la forma exacta del incidente de `cleanup_raw_orphans` (2026-08-03)
    y de la regresión de los globs `cache_*` vs `raw.`: dos formas del
    mismo nombre. Actuar acá re-descarga el catálogo entero.
    """
    from app.infrastructure.celery.tasks.collector_tasks import _orphan_guard_tripped

    assert _orphan_guard_tripped(30_921, 30_921) is True
    assert _orphan_guard_tripped(20_000, 30_921) is True


def test_con_un_catalogo_chico_manda_el_piso_absoluto() -> None:
    """El 10 % de un catálogo chico es un número minúsculo.

    Sin el piso, 4 huérfanas sobre 30 filas dispararían la guarda y la
    tarea no arreglaría nunca nada en un entorno recién levantado.
    """
    from app.infrastructure.celery.tasks.collector_tasks import _orphan_guard_tripped

    assert _orphan_guard_tripped(4, 30) is False
    assert _orphan_guard_tripped(400, 500) is False


@pytest.mark.parametrize("total", [0, 1, 10_000, 30_921])
def test_la_guarda_nunca_explota(total: int) -> None:
    from app.infrastructure.celery.tasks.collector_tasks import _orphan_guard_tripped

    for candidatos in (0, 1, total, total * 2):
        assert isinstance(_orphan_guard_tripped(candidatos, total), bool)
