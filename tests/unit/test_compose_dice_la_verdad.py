"""El compose del repo tiene que describir lo que corre en los servidores.

Hasta el 2026-09-28 no lo hacía. El compose real vivía suelto en
`/opt/docker/openarg` de cada máquina, sin git y con ocho copias `.bak` al
lado, y el del repo se había quedado atrás. Comparados los tres archivos,
el repo difería en límites de memoria, concurrencia del colector, una
variable de entorno y —lo más serio— en la política de desalojo de Redis.

Eso no es cosmético: **el test de colas ya parsea este archivo** para saber
qué `-Q` consume cada worker. Si el archivo miente, ese test valida contra
una ficción. Fue justamente así como `bulk_collect_all` quedó sin
consumidor en el repo mientras los dos servidores sí la consumían.

Staging y prod son idénticos entre sí salvo por el nombre del stack y el
tag de imagen, ambos parametrizados acá.
"""

from __future__ import annotations

from pathlib import Path

import pytest

_COMPOSE = Path("docker-compose.prod.yml").read_text(encoding="utf-8")


def test_redis_no_descarta_claves_cuando_se_llena():
    """La diferencia entre perder tareas en silencio y que el fallo se vea.

    Las claves de este Redis son la cola de Celery. Con `allkeys-lru` —que
    era lo que decía el repo— Redis descarta claves al llenarse, o sea
    tareas encoladas, sin un solo error. Con `noeviction` rechaza la
    escritura y el problema aparece. El 2026-09-09 staging se llenó con
    432.077 tareas duplicadas y el síntoma fueron escrituras fallando, que
    es exactamente lo que se quiere.
    """
    comando = next(
        linea for linea in _COMPOSE.splitlines() if "redis-server --requirepass" in linea
    )
    assert "--maxmemory-policy noeviction" in comando
    # Sobre la línea del comando y no sobre el archivo entero: el comentario
    # que explica la decisión nombra `allkeys-lru`, y buscarlo suelto hace
    # fallar al test por su propia documentación.
    assert "allkeys-lru" not in comando


def test_el_tag_de_imagen_es_parametrizable():
    """Un solo archivo para los dos entornos: lo único que cambia es el tag.

    Prod consume `:latest` (que sólo publica `main`) y staging `:staging`.
    Hardcodear uno obliga a mantener dos archivos divergentes, que es
    exactamente el problema que este PR cierra.
    """
    nuestras = [
        linea.strip()
        for linea in _COMPOSE.splitlines()
        if "image:" in linea and "/openarg/" in linea
    ]
    assert len(nuestras) >= 12, f"se esperaban las 12 imágenes propias, hay {len(nuestras)}"
    # Sólo las imágenes propias: `pgbouncer:latest` y `caddy:2-alpine` son de
    # terceros y su tag no lo decide este repo.
    hardcodeadas = [x for x in nuestras if "${OPENARG_IMAGE_TAG" not in x]
    assert not hardcodeadas, f"tags de imagen propia sin parametrizar: {hardcodeadas}"


def test_el_nombre_del_stack_es_parametrizable():
    assert "name: openarg-${OPENARG_STACK:-prod}" in _COMPOSE


@pytest.mark.parametrize(
    ("servicio", "cola"),
    [
        ("worker-ingest", "ingest,orchestrator"),
        ("worker-collector", "collector"),
    ],
)
def test_las_colas_del_compose_son_las_que_corren(servicio: str, cola: str):
    """`orchestrator` es el caso que motivó esto.

    Los dos servidores consumían `ingest,orchestrator` mientras el
    Dockerfile decía sólo `ingest`: recrear el stack desde el repo dejaba
    `bulk_collect_all` sin consumidor, y nada lo habría avisado.
    """
    assert f'"-Q", "{cola}"' in _COMPOSE or f"-Q {cola}" in _COMPOSE


def test_el_colector_arranca_con_la_concurrencia_real():
    """El default era 2 y en los dos servidores corre 8."""
    assert "${OPENARG_COLLECTOR_CONCURRENCY:-8}" in _COMPOSE
