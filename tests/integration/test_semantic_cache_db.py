"""El caché semántico contra pgvector de verdad.

`SemanticCache.get` se traga cualquier error de SQL y lo devuelve como "no
hay nada": una consulta rota se ve igual que un caché vacío. Un doble en memoria
no lo distingue, por eso los aciertos se prueban acá.

Los vectores son sintéticos, para fijar la distancia: lo que se prueba es qué
hace el caché con dos preguntas que están cerca, no cuánto se parecen.
"""

from __future__ import annotations

import math
import os
import uuid

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.infrastructure.adapters.cache.semantic_cache import SemanticCache

_DIMS = 1024


def _url_or_skip() -> str:
    url = os.getenv("DATABASE_URL", "")
    if not url:
        pytest.skip("DATABASE_URL not set — este test necesita una DB real")
    try:
        engine = create_engine(url, pool_pre_ping=True)
        with engine.connect() as conn:
            exists = conn.execute(text("SELECT to_regclass('public.query_cache')")).scalar()
        engine.dispose()
    except Exception as exc:  # pragma: no cover — environmental
        pytest.skip(f"DB unreachable: {exc}")
    if exists is None:
        pytest.skip("query_cache no existe: la base no está migrada")
    return url


def _vec(similarity: float) -> list[float]:
    """Un vector con esa similitud coseno contra `_vec(1.0)`."""
    return [similarity, math.sqrt(1 - similarity**2), *([0.0] * (_DIMS - 2))]


@pytest.fixture
async def cache():
    url = _url_or_skip()
    engine = create_async_engine(url)
    factory = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    marca = uuid.uuid4().hex
    yield SemanticCache(factory), marca
    async with factory() as session:
        await session.execute(
            text("DELETE FROM query_cache WHERE question LIKE :m"), {"m": f"%{marca}%"}
        )
        await session.commit()
    await engine.dispose()


async def test_near_question_with_the_same_terms_is_served(cache):
    semantic, marca = cache
    await semantic.set(f"¿Cuál es el PBI de {marca}?", _vec(0.99), {"answer": "pbi"})
    # Otro hash: no es el acierto exacto el que responde.
    found = await semantic.get(f"Cual es el PBI de {marca}", embedding=_vec(1.0))
    assert found == {"answer": "pbi"}


async def test_near_question_for_another_figure_is_not_served(cache):
    semantic, marca = cache
    await semantic.set(f"Variación interanual del IPC {marca}", _vec(0.99), {"answer": "33,19 %"})
    found = await semantic.get(f"Índice del IPC {marca}", embedding=_vec(1.0))
    assert found is None


async def test_the_closest_question_can_ask_for_another_figure(cache):
    semantic, marca = cache
    await semantic.set(
        f"Variación interanual del IPC {marca}", _vec(0.99), {"answer": "interanual"}
    )
    await semantic.set(
        f"¿Cuál fue la variación mensual del IPC {marca}?", _vec(0.95), {"answer": "mensual"}
    )
    found = await semantic.get(f"Variación mensual del IPC {marca}", embedding=_vec(1.0))
    assert found == {"answer": "mensual"}


async def test_below_the_threshold_is_not_served(cache):
    semantic, marca = cache
    await semantic.set(f"¿Cuál es el PBI de {marca}?", _vec(0.80), {"answer": "pbi"})
    found = await semantic.get(f"Cual es el PBI de {marca}", embedding=_vec(1.0))
    assert found is None
