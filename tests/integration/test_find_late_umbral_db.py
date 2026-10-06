"""El umbral de `find_late` contra Postgres: es SQL, y con un doble no se prueba.

Una tarea mensual que dejaba de correr tardaba 90 días en ser "tarde" (3 x su
cadencia) y necesitaba 4 corridas de historia antes de poder juzgarse. El techo
de una semana sobre la cadencia y la cadencia declarada del beat lo bajan a ~38
días desde la primera corrida. Cada test usa identidades `zz_prueba_<hex>` y las
borra al terminar.
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest
from sqlalchemy import text

from app.application.quality.heartbeat import find_late
from app.infrastructure.celery.tasks import _db


def _engine_or_skip():
    if not os.getenv("DATABASE_URL"):
        pytest.skip("DATABASE_URL not set — este test necesita una DB real")
    try:
        engine = _db.get_sync_engine()
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")).scalar()
        return engine
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")


@pytest.fixture
def latidos():
    engine = _engine_or_skip()
    pfx = f"zz_prueba_{uuid.uuid4().hex[:8]}"
    find_late(engine)  # crea la tabla si falta

    def _insertar(rid: str, dias_atras: float, cadencia_dias: float | None, veces: int) -> None:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO public.ingest_heartbeat "
                    "(resource_identity, last_ok_at, cadence_seconds, times_seen) "
                    "VALUES (:r, now() - make_interval(secs => :s), :c, :v)"
                ),
                {
                    "r": rid,
                    "s": dias_atras * 86400,
                    "c": cadencia_dias * 86400 if cadencia_dias else None,
                    "v": veces,
                },
            )

    yield engine, pfx, _insertar

    with engine.begin() as conn:
        conn.execute(
            text("DELETE FROM public.ingest_heartbeat WHERE resource_identity LIKE :p"),
            {"p": f"%{pfx}%"},
        )


def _tarde(engine, pfx: str, **kw: Any) -> set[str]:
    return {
        x.resource_identity
        for x in find_late(engine, limit=10_000, **kw)
        if pfx in x.resource_identity
    }


def test_una_mensual_que_no_llega_hace_40_dias_esta_tarde(latidos):
    # Antes: 3 x 30 = 90 días. Con el techo de una semana sobre la cadencia, ~37.
    engine, pfx, insertar = latidos
    insertar(f"{pfx}::mensual_atrasada", 40, 30, 4)
    insertar(f"{pfx}::mensual_al_dia", 20, 30, 4)
    insertar(f"{pfx}::diaria_atrasada", 4, 1, 10)
    assert _tarde(engine, pfx) == {f"{pfx}::mensual_atrasada", f"{pfx}::diaria_atrasada"}


def test_una_tarea_con_cadencia_declarada_se_juzga_desde_la_primera_corrida(latidos):
    engine, pfx, insertar = latidos
    tarea = f"task:openarg.{pfx}"
    insertar(tarea, 4, None, 1)  # una sola corrida: sin historia que aprender
    assert _tarde(engine, pfx) == set(), "sin declarar, una corrida no alcanza"
    assert _tarde(engine, pfx, declared={tarea: 86400.0}) == {tarea}
