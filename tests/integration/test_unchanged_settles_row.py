"""Una recolección que no tuvo trabajo que hacer no puede dejar la fila abierta.

El 2026-09-09, con `recover_stuck_tasks` recién destrabada, aparecieron 194
filas de `raw.cached_datasets` clavadas en `downloading` desde el 26-ago.
No era que el colector se muriera a mitad: **terminaba bien y no cerraba
la fila**.

`_ensure_cached_entry` reserva la fila con `status='downloading'` y
`table_name` = el nombre de la PRÓXIMA versión (`..._v2`), porque corre
antes de saber si el archivo cambió. Cuando el sha256 resultaba idéntico
al de la versión viva, el camino `unchanged` salía temprano tocando sólo
`updated_at`: la `_v2` no se creaba nunca y la fila quedaba apuntando a
una tabla inexistente, sin error, en `downloading` — mientras la fila sana
del mismo dataset seguía en `ready` sobre `..._v1`. 193 de las 194
hermanas estaban así.

Y no se quedaban quietas: `_recycle_stuck_downloads` las veía stale a los
30 minutos, las degradaba y las re-despachaba; el colector volvía a decir
"unchanged" y a refrescar `updated_at`. Un ciclo de 45 minutos que subía
`retry_count` en cada vuelta hasta `permanently_failed`, quemando un
dataset perfectamente descargado y pagando una descarga HTTP cada vez.

El 2026-09-09, ya con el arreglo desplegado, aparecieron 7 filas nuevas
en el mismo estado. El culpable era **el otro** camino de salida
temprana: `already_appended` —las filas de este dataset ya están en la
tabla destino— marcaba `datasets.is_cached` y se iba igual de temprano.
De ahí que la función se llame `_settle_reserved_row` y no
`_settle_unchanged_row`: lo que cierra es la fila que dejó la reserva,
venga de donde venga.

Estos tests corren contra Postgres de verdad porque lo que decide entre
las dos ramas es `uq_cached_datasets_table_name`: con un doble en memoria
la restricción no existe y el test no probaría nada.
"""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from app.infrastructure.celery.tasks.collector_tasks import _settle_reserved_row


def _engine_or_skip():
    url = os.getenv("DATABASE_URL", "")
    if not url:
        pytest.skip("DATABASE_URL not set — este test necesita una DB real")
    try:
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        return engine
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")


@pytest.fixture
def cached(request):
    """Una `raw.cached_datasets` descartable, con el UNIQUE que importa."""
    engine = _engine_or_skip()
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw"))
        conn.execute(text("DROP TABLE IF EXISTS raw.cached_datasets CASCADE"))
        conn.execute(
            text(
                """
                CREATE TABLE raw.cached_datasets (
                    id serial PRIMARY KEY,
                    dataset_id uuid NOT NULL,
                    table_name text,
                    status text,
                    error_message text,
                    updated_at timestamptz,
                    CONSTRAINT uq_cached_datasets_table_name UNIQUE (table_name)
                )
                """
            )
        )

    def _cleanup():
        with engine.begin() as conn:
            conn.execute(text("DROP TABLE IF EXISTS raw.cached_datasets CASCADE"))

    request.addfinalizer(_cleanup)
    return engine


def _insert(engine, did, table, status, *, error="algo", stale_days=9):
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO raw.cached_datasets (dataset_id, table_name, status, "
                "error_message, updated_at) VALUES (CAST(:d AS uuid), :t, :s, :e, "
                "NOW() - (:days * INTERVAL '1 day'))"
            ),
            {"d": did, "t": table, "s": status, "e": error, "days": stale_days},
        )


def _rows(engine, did):
    with engine.connect() as conn:
        return {
            r.table_name: (r.status, r.error_message)
            for r in conn.execute(
                text(
                    "SELECT table_name, status, error_message FROM raw.cached_datasets "
                    "WHERE dataset_id = CAST(:d AS uuid)"
                ),
                {"d": did},
            )
        }


def test_el_duplicado_reservado_se_borra(cached):
    """El caso de las 194: la hermana sana ya ocupa el nombre vivo."""
    did = str(uuid.uuid4())
    _insert(cached, did, "t__abc__v1", "ready", error=None)
    _insert(cached, did, "t__abc__v2", "downloading", error=None)

    _settle_reserved_row(
        cached, dataset_id=did, reserved_table="t__abc__v2", live_table="t__abc__v1"
    )

    filas = _rows(cached, did)
    assert "t__abc__v2" not in filas, "la fila reservada sobra: su versión nunca se creó"
    assert filas["t__abc__v1"][0] == "ready"
    assert len(filas) == 1


def test_sin_hermana_la_fila_converge_al_nombre_vivo(cached):
    """Sin duplicado, la reservada ES la del recurso y sólo tiene mal el nombre.

    Borrarla acá perdería la única fila del dataset.
    """
    did = str(uuid.uuid4())
    _insert(cached, did, "t__abc__v2", "downloading")

    _settle_reserved_row(
        cached, dataset_id=did, reserved_table="t__abc__v2", live_table="t__abc__v1"
    )

    filas = _rows(cached, did)
    assert list(filas) == ["t__abc__v1"]
    assert filas["t__abc__v1"] == ("ready", None)


def test_cuando_la_reserva_ya_es_la_viva_solo_se_cierra(cached):
    """El archivo no cambió y el nombre reservado coincide: nada que mover."""
    did = str(uuid.uuid4())
    _insert(cached, did, "t__abc__v1", "downloading")

    _settle_reserved_row(
        cached, dataset_id=did, reserved_table="t__abc__v1", live_table="t__abc__v1"
    )

    assert _rows(cached, did) == {"t__abc__v1": ("ready", None)}


def test_no_se_toca_otro_dataset(cached):
    """El borrado se ancla al dataset y al nombre reservado, no al estado."""
    mio, ajeno = str(uuid.uuid4()), str(uuid.uuid4())
    _insert(cached, mio, "t__abc__v1", "ready", error=None)
    _insert(cached, mio, "t__abc__v2", "downloading", error=None)
    _insert(cached, ajeno, "otro__xyz__v1", "downloading", error=None)

    _settle_reserved_row(
        cached, dataset_id=mio, reserved_table="t__abc__v2", live_table="t__abc__v1"
    )

    assert _rows(cached, ajeno) == {"otro__xyz__v1": ("downloading", None)}


def test_una_fila_que_no_esta_descargando_no_se_borra(cached):
    """Sólo se recoge lo que la reserva dejó abierto, no cualquier homónimo."""
    did = str(uuid.uuid4())
    _insert(cached, did, "t__abc__v1", "ready", error=None)
    _insert(cached, did, "t__abc__v2", "permanently_failed", error="viejo")

    _settle_reserved_row(
        cached, dataset_id=did, reserved_table="t__abc__v2", live_table="t__abc__v1"
    )

    filas = _rows(cached, did)
    assert filas["t__abc__v2"] == ("permanently_failed", "viejo")
    assert filas["t__abc__v1"][0] == "ready"


def test_corre_dos_veces_sin_cambiar_nada_mas(cached):
    """El colector la llama en cada recolección sin cambios: tiene que ser idempotente."""
    did = str(uuid.uuid4())
    _insert(cached, did, "t__abc__v1", "ready", error=None)
    _insert(cached, did, "t__abc__v2", "downloading", error=None)

    for _ in range(2):
        _settle_reserved_row(
            cached, dataset_id=did, reserved_table="t__abc__v2", live_table="t__abc__v1"
        )

    assert _rows(cached, did) == {"t__abc__v1": ("ready", None)}


def test_los_dos_caminos_de_salida_temprana_cierran_la_fila() -> None:
    """Ninguna salida temprana puede quedarse con la fila abierta.

    `unchanged` se arregló en el #59 y `already_appended` seguía suelto: la
    tarea reportaba `succeeded` y la fila quedaba en `downloading` hasta que
    `_recycle_stuck_downloads` la agotaba hasta `permanently_failed`. Si
    mañana aparece un tercer camino, que este test lo obligue a cerrar.
    """
    fuente = Path("src/app/infrastructure/celery/tasks/collector_tasks.py").read_text(
        encoding="utf-8"
    )
    for salida in ('"status": "unchanged"', 'return target_table, True, "already_appended"'):
        i = fuente.index(salida)
        # El cierre tiene que estar cerca y antes de irse, no en otra función.
        assert "_settle_reserved_row(" in fuente[max(0, i - 2500) : i + 200], salida
