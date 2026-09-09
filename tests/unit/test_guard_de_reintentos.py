"""El presupuesto de intentos es de la fila, no del dataset.

`collect_data` chequea `retry_count` antes de bajar nada, para no
reintentar para siempre un recurso que falla. La consulta era:

    SELECT retry_count FROM raw.cached_datasets WHERE dataset_id = :did

Sin filtro de nombre y sin `ORDER BY`. Un dataset tiene **una fila por
`table_name`** —una por versión del recurso— así que con más de una,
`fetchone()` devolvía cualquiera: el mismo dataset se procesaba o se
saltaba según el plan de Postgres.

Medido en staging el 2026-09-09: 32.868 datasets, 1.935 con más de una
fila (hasta 24), y **23 con una fila agotada junto a otra sana**. En uno
de ellos la agotada era la `v10` y la sana la `v7`, o sea que ni siquiera
alcanzaba con quedarse con la versión más nueva.

El efecto: una versión vieja fallida bloqueaba el dataset entero, y como
el `return` tampoco cerraba la fila que `_ensure_cached_entry` acababa de
reservar, cada intento dejaba otra fila colgada en `downloading` — el
tercer sitio con ese mismo defecto, después de `unchanged` (#59) y
`already_appended` (#62).
"""

from __future__ import annotations

from pathlib import Path

_FUENTE = Path("src/app/infrastructure/celery/tasks/collector_tasks.py").read_text(encoding="utf-8")


def _bloque_del_guard() -> str:
    i = _FUENTE.index("# Check retry_count — skip if permanently failed")
    return _FUENTE[i : i + 2600]


def test_el_guard_pregunta_por_la_fila_que_va_a_trabajar() -> None:
    bloque = _bloque_del_guard()

    assert "AND table_name = :tn" in bloque, (
        "sin el nombre, `fetchone()` devuelve una fila cualquiera del dataset "
        "y el bloqueo depende del plan de Postgres"
    )
    assert '"tn": table_name' in bloque


def test_al_bloquear_cierra_la_fila_que_reservo() -> None:
    """`_ensure_cached_entry` ya la puso en `downloading` unas líneas antes.

    Un `return` sin cerrar la deja abierta, y de ahí la levanta
    `_recycle_stuck_downloads` para reciclarla en un ciclo que no lleva a
    ninguna parte: el dataset está bloqueado por este mismo guard.
    """
    bloque = _bloque_del_guard()
    i = bloque.index('return {"error": "permanently_failed"}')

    assert "UPDATE raw.cached_datasets" in bloque[:i]
    assert "SET status = 'permanently_failed'" in bloque[:i]
    # Y sólo sobre la fila reservada por este intento.
    assert "AND status = 'downloading'" in bloque[:i]


def test_el_cierre_no_pisa_un_error_previo() -> None:
    """El motivo real de la falla vale más que el genérico de este guard."""
    bloque = _bloque_del_guard()

    assert "coalesce(" in bloque and "error_message" in bloque


def test_ninguna_salida_temprana_del_colector_deja_la_fila_abierta() -> None:
    """Las tres, juntas, para que una cuarta nazca cubierta.

    `unchanged` (#59) y `already_appended` (#62) cierran con
    `_settle_reserved_row`; este guard cierra con su propio UPDATE porque
    el estado correcto acá es `permanently_failed`, no `ready`.
    """
    salidas = {
        '"status": "unchanged"': "_settle_reserved_row(",
        'return target_table, True, "already_appended"': "_settle_reserved_row(",
        'return {"error": "permanently_failed"}': "UPDATE raw.cached_datasets",
    }
    for salida, cierre in salidas.items():
        i = _FUENTE.index(salida)
        assert cierre in _FUENTE[max(0, i - 2500) : i + 200], salida
