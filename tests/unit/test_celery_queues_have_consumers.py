"""Toda cola a la que se despacha tiene que tener quién la consuma.

El 2026-09-09, diez filas de `cached_datasets` quedaron trabadas en
`downloading` durante horas. La tarea que existe justamente para
destrabarlas, `openarg.recover_stuck_tasks`, se despachaba puntualmente
cada 15 minutos —y no corría nunca—: iba a la cola `default`, que **no
figura en el `-Q` de ningún worker**. La cola tenía 7.875 mensajes
acumulados y no bajaba entre muestras de 20 segundos.

Es una falla especialmente cara de detectar porque todo *parece* andar:
la tarea está registrada, beat la agenda, el despacho no da error y el
mensaje se encola bien. Sólo falta el consumidor, y de eso no se queja
nadie. Encima Celery no avisa: `task_default_queue` es `scraper`, así
que el problema no es "no hay cola default", es que la hay y está muerta.

Este test cierra el lazo entre las dos mitades de la configuración, que
viven en repos de archivos distintos: quién despacha (`task_routes` y
`beat_schedule`, en Python) y quién consume (los `-Q` de
`docker/*.Dockerfile` y `docker-compose.prod.yml`).

Alcance, para no confiar de más en el verde: lo consumido se lee del
**repo**, y el compose que corre de verdad vive en `/opt/docker/openarg`
de cada servidor, sin versionar. Escribiendo este test se descubrió que
los dos entornos consumían `ingest,orchestrator` mientras el Dockerfile
decía sólo `ingest` —o sea que recrear el stack desde el repo dejaba
`bulk_collect_all` sin consumidor—; se alineó el Dockerfile. El test
cubre la deriva dentro del repo. La deriva repo ↔ servidor sigue siendo
un agujero, y sólo se cierra versionando el compose.

`known_orphan_queue_tasks.txt` congela lo que ya estaba roto cuando este
test aterrizó, igual que `known_broken_hints.txt` en
`test_dataset_index.py`: el test falla ante deriva NUEVA y ante entradas
que ya se arreglaron, así que el archivo sólo puede achicarse.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_RAIZ = Path(__file__).resolve().parents[2]
_FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"


def _colas_consumidas() -> set[str]:
    """Las colas que algún worker realmente escucha.

    Se leen del `-Q` de cada worker. En compose el valor puede venir como
    `${VAR:-cola}`; lo que importa es el default, que es lo que corre en
    staging y prod salvo que alguien setee la variable.
    """
    colas: set[str] = set()
    dockerfiles = list((_RAIZ / "docker").glob("*.Dockerfile"))
    composes = list(_RAIZ.glob("docker-compose*.yml"))
    # Las dos mitades tienen que estar: los workers dedicados salen de los
    # Dockerfile y los de colector pesado del compose. Con una sola mitad el
    # test seguiría en verde midiendo menos de lo que cree medir.
    assert dockerfiles, "no se encontró ningún docker/*.Dockerfile que parsear"
    assert composes, "no se encontró ningún docker-compose*.yml que parsear"
    fuentes = dockerfiles + composes

    for archivo in fuentes:
        texto = archivo.read_text(encoding="utf-8")
        # Forma Dockerfile: "-Q", "ingest,orchestrator"  |  compose: -Q collector
        # La coma NO se excluye acá: un solo worker puede escuchar varias colas
        # y recortar en la primera coma dejaba `orchestrator` fuera de la cuenta.
        for bruto in re.findall(r'-Q"?,?\s+"?([^\s"]+)', texto):
            # `${OPENARG_HEAVY_COLLECT_QUEUE:-collector-heavy}` → collector-heavy
            var = re.fullmatch(r"\$\{[^:}]+:-([^}]+)\}", bruto)
            if var:
                bruto = var.group(1)
            elif bruto.startswith("$"):
                continue  # sin default declarado: no se puede afirmar nada
            colas.update(p for p in bruto.split(",") if p)
    return colas


def _colas_despachadas() -> dict[str, str]:
    """Tarea → cola efectiva.

    El orden importa y es el de Celery: `options` de una entrada de beat
    **pisa** `task_routes`. Ignorarlo fue justamente lo que casi deja el
    arreglo a medias — `recover_stuck_tasks` estaba en `default` en los
    dos lados, y corregir sólo el ruteo no habría cambiado nada.
    """
    from app.infrastructure.celery.app import celery_app

    destino: dict[str, str] = {}
    for tarea, ruta in (celery_app.conf.task_routes or {}).items():
        if isinstance(ruta, dict) and ruta.get("queue"):
            destino[tarea] = ruta["queue"]
    for entrada in (celery_app.conf.beat_schedule or {}).values():
        cola = (entrada.get("options") or {}).get("queue")
        if cola:
            destino[entrada["task"]] = cola
    return destino


def _baseline() -> set[str]:
    texto = (_FIXTURES / "known_orphan_queue_tasks.txt").read_text(encoding="utf-8")
    return {
        linea.strip()
        for linea in texto.splitlines()
        if linea.strip() and not linea.lstrip().startswith("#")
    }


def test_la_cola_default_no_la_consume_nadie() -> None:
    """El hecho concreto detrás de todo lo demás, escrito aparte.

    Si algún día alguien levanta un worker sobre `default`, este test se
    pone en rojo y hay que revisar el baseline entero: dejaría de haber
    motivo para que ninguna tarea esté ahí listada.
    """
    assert "default" not in _colas_consumidas()


def test_toda_cola_despachada_tiene_consumidor() -> None:
    consumidas = _colas_consumidas()
    assert "ingest" in consumidas and "collector" in consumidas, (
        f"el parseo de los `-Q` no encontró las colas conocidas: {sorted(consumidas)}"
    )

    huerfanas = {
        f"{tarea} -> {cola}"
        for tarea, cola in _colas_despachadas().items()
        if cola not in consumidas
    }
    baseline = _baseline()

    nuevas = sorted(huerfanas - baseline)
    assert not nuevas, (
        "Estas tareas se despachan a una cola que ningún worker consume, o sea "
        "que se encolan y no corren nunca. Arreglá el ruteo; NO agregues "
        "entradas nuevas a known_orphan_queue_tasks.txt:\n" + "\n".join(nuevas)
    )

    resueltas = sorted(baseline - huerfanas)
    assert not resueltas, (
        "Ya arregladas pero todavía listadas en known_orphan_queue_tasks.txt — "
        "borralas para que el baseline sólo se achique:\n" + "\n".join(resueltas)
    )


def test_recover_stuck_tasks_corre_en_una_cola_viva() -> None:
    """La regresión puntual que originó todo esto.

    Se afirma sobre el destino *efectivo*, no sobre `task_routes`: es la
    diferencia entre comprobar que la tarea está bien escrita y comprobar
    que llega a un worker.
    """
    destino = _colas_despachadas()["openarg.recover_stuck_tasks"]
    assert destino in _colas_consumidas(), (
        f"`recover_stuck_tasks` despacha a `{destino}`, que nadie consume: "
        "las descargas trabadas se quedan trabadas"
    )


@pytest.mark.parametrize(
    "tarea",
    ["openarg.recover_stuck_tasks", "openarg.cleanup_orphan_catalog_entries"],
)
def test_las_tareas_de_mantenimiento_estan_agendadas(tarea: str) -> None:
    """Ruteada a una cola viva pero sin agendar tampoco corre sola."""
    from app.infrastructure.celery.app import celery_app

    agendadas = {e["task"] for e in (celery_app.conf.beat_schedule or {}).values()}
    assert tarea in agendadas
