"""Una consulta sin conversación tiene que poder contestarse.

Un grafo de LangGraph compilado **con** checkpointer rechaza cualquier
invocación que no traiga `thread_id`:

    ValueError: Checkpointer requires one or more of the following
    'configurable' keys: thread_id, checkpoint_ns, checkpoint_id

El router sólo lo pasaba cuando había `conversation_id`:

    if checkpointer and conversation_id:
        invoke_config["configurable"] = {"thread_id": conversation_id}

Así que un request sin conversación salía por el `except` genérico como
**500 PIPELINE_ERROR**, un mensaje del que no se deduce nada. Desde el
frontend no se nota porque siempre manda `conversation_id`.

Apareció al arreglar el E2E, que llevaba desde el 2026-08-01 fallando por
otra causa (un secret con el endpoint viejo del RDS) y tapaba esto: 132 de
135 tests caían con el mismo 500. Es el argumento entero a favor de no
dejar una suite en rojo permanente — no se pierde la suite, se pierde todo
lo que la suite habría encontrado.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_ROUTER = Path("src/app/presentation/http/controllers/query/smart_query_v2_router.py").read_text(
    encoding="utf-8"
)


def _bloques_de_config() -> list[str]:
    """Los dos sitios que arman el `configurable`: POST /smart y el WebSocket."""
    return [
        _ROUTER[m.start() - 700 : m.end() + 200]
        for m in re.finditer(r'\["configurable"\] = \{|"configurable"\] = \{', _ROUTER)
    ]


def test_hay_dos_caminos_que_invocan_el_grafo():
    """POST y WebSocket. Arreglar uno solo deja el bug vivo en el otro."""
    assert len(_bloques_de_config()) == 2


@pytest.mark.parametrize("i", [0, 1])
def test_el_thread_id_no_depende_de_que_haya_conversacion(i: int):
    bloque = _bloques_de_config()[i]

    assert "if checkpointer and conversation_id:" not in bloque, (
        "con checkpointer activo y sin conversación, el grafo rechaza la "
        "invocación y el usuario recibe un 500 genérico"
    )
    assert "conversation_id or" in bloque, "falta el thread efímero para consultas sueltas"


@pytest.mark.parametrize("i", [0, 1])
def test_la_conversacion_real_sigue_mandando(i: int):
    """El efímero es el respaldo, no el camino principal.

    Si pisara al `conversation_id`, cada mensaje abriría un hilo nuevo y se
    perdería el historial — un bug peor que el que se arregla.
    """
    bloque = _bloques_de_config()[i]
    m = re.search(r'"thread_id":\s*(conversation_id[^,}]*)', bloque)
    assert m, "el thread_id ya no se deriva de conversation_id"
    assert m.group(1).startswith("conversation_id or"), (
        f"conversation_id tiene que ir primero, no {m.group(1)!r}"
    )
