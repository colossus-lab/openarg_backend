"""Los locks del router no pueden atarse al primer event loop que los toque.

`asyncio.Lock()` creado a nivel de módulo se liga al primer loop que lo
usa; desde ahí, cualquier otro recibe:

    RuntimeError: <asyncio.locks.Lock object> is bound to a different event loop

En producción no se nota —uvicorn corre un loop por proceso y el módulo se
importa una vez— pero basta un segundo loop para que **todo** request falle
con un 500 genérico, porque el lock protege la init perezosa del
checkpointer y del grafo compilado.

Así estaba la suite E2E, donde cada test crea su propio loop: **131 de 135
fallaban** por esto. Y el síntoma era `PIPELINE_ERROR`, que no menciona
locks ni loops.

Vale la historia: el E2E llevaba desde el 2026-08-01 en rojo por un secret
con el endpoint viejo del RDS. Arreglado eso apareció un bug de `thread_id`
(PR #71), y arreglado ése apareció éste. Tres capas, cada una tapando a la
siguiente.
"""

from __future__ import annotations

import asyncio

from app.presentation.http.controllers.query import smart_query_v2_router as router


def test_cada_loop_recibe_su_propio_lock():
    """Dos loops, dos locks. Es lo que el módulo no hacía.

    Los dos loops se mantienen vivos a la vez y se comparan los objetos, no
    sus `id()`: con `asyncio.run()` el primer lock se libera antes de crear el
    segundo y CPython reutiliza la dirección, así que comparar ids da un falso
    negativo aunque el arreglo esté bien.
    """

    async def tomar():
        lock = router._lock("checkpointer")
        async with lock:
            pass
        return lock

    loop1 = asyncio.new_event_loop()
    loop2 = asyncio.new_event_loop()
    try:
        primero = loop1.run_until_complete(tomar())
        segundo = loop2.run_until_complete(tomar())
        assert primero is not segundo, (
            "el mismo lock en dos loops distintos es exactamente el error que "
            "rompía 131 de 135 tests E2E"
        )
    finally:
        loop1.close()
        loop2.close()


def test_el_mismo_loop_reutiliza_su_lock():
    """Si no, el lock no sincroniza nada: cada llamada tomaría uno nuevo."""

    async def dos_veces() -> tuple[int, int]:
        return id(router._lock("checkpointer")), id(router._lock("checkpointer"))

    a, b = asyncio.run(dos_veces())
    assert a == b


def test_cada_nombre_tiene_su_lock():
    """`checkpointer` y `compiled_graphs` protegen cosas distintas."""

    async def dos_nombres() -> tuple[int, int]:
        return id(router._lock("checkpointer")), id(router._lock("compiled_graphs"))

    a, b = asyncio.run(dos_nombres())
    assert a != b


def test_no_quedan_locks_de_modulo():
    """Un `asyncio.Lock()` en el import vuelve a traer el bug entero."""
    import inspect
    import re

    fuente = inspect.getsource(router)
    # Una ASIGNACIÓN sin indentar, no cualquier mención: el comentario que
    # explica esta decisión nombra `asyncio.Lock()`, y buscarlo suelto hace
    # fallar al test por su propia documentación. Los locks creados dentro de
    # una corrutina (como el `send_lock` del stream) son correctos.
    de_modulo = re.findall(r"^_\w+\s*=\s*asyncio\.Lock\(\)", fuente, re.MULTILINE)
    assert not de_modulo, f"locks creados en tiempo de import: {de_modulo} — se atan al primer loop"
