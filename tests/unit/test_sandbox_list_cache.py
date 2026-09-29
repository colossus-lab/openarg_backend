"""La caché de `list_cached_tables()` tiene que sobrevivir entre instancias.

Medido en staging el 29-sep-2026: `SandboxProvider` entrega el adapter con
`Scope.REQUEST`, así que cada consulta del chat y de /api/v1/ask armaba un
`PgSandboxAdapter` nuevo con la caché vacía, y pagaba el listado completo
(~32.400 filas, 3,9–4,8 s) aunque otra consulta lo hubiera hecho segundos
antes. La caché ahora vive a nivel de módulo, con el mismo TTL de 60 s.
"""

from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from app.domain.ports.sandbox.sql_sandbox import CachedTableInfo
from app.infrastructure.adapters.sandbox import pg_sandbox_adapter as psa
from app.infrastructure.adapters.sandbox.pg_sandbox_adapter import PgSandboxAdapter


@pytest.fixture(autouse=True)
def _cache_limpia(monkeypatch):
    monkeypatch.setenv("SANDBOX_DATABASE_URL", "postgresql+psycopg://u:p@h:5432/db")
    psa._LIST_CACHE.clear()
    yield
    psa._LIST_CACHE.clear()


class _Listado:
    """Reemplaza `_list_tables_sync`: cuenta las veces que se lista de verdad."""

    def __init__(self, demora_s: float = 0.0) -> None:
        self.llamadas = 0
        self.demora_s = demora_s
        self._lock = threading.Lock()

    def __call__(self, _adapter: PgSandboxAdapter) -> list[CachedTableInfo]:
        with self._lock:
            self.llamadas += 1
            n = self.llamadas
        if self.demora_s:
            time.sleep(self.demora_s)
        return [CachedTableInfo(table_name=f"cache_t{n}", dataset_id="d", row_count=1, columns=[])]


class _Reloj:
    def __init__(self) -> None:
        self.ahora = 1000.0

    def monotonic(self) -> float:
        return self.ahora


@pytest.fixture
def listado(monkeypatch) -> _Listado:
    fake = _Listado()
    # Función y no la instancia: un callable que no es función no se liga
    # como método, y `self` no llegaría.
    monkeypatch.setattr(PgSandboxAdapter, "_list_tables_sync", lambda self: fake(self))
    return fake


@pytest.fixture
def reloj(monkeypatch) -> _Reloj:
    # Se reemplaza la referencia `time` del módulo del adapter, no
    # `time.monotonic` global: el event loop de asyncio también lo usa.
    r = _Reloj()
    monkeypatch.setattr(psa, "time", SimpleNamespace(monotonic=r.monotonic))
    return r


async def test_dos_instancias_comparten_la_cache_dentro_del_ttl(listado, reloj) -> None:
    primera = await PgSandboxAdapter().list_cached_tables()
    reloj.ahora += 59
    segunda = await PgSandboxAdapter().list_cached_tables()

    assert listado.llamadas == 1
    assert [t.table_name for t in segunda] == [t.table_name for t in primera] == ["cache_t1"]


async def test_vencido_el_ttl_se_vuelve_a_listar(listado, reloj) -> None:
    await PgSandboxAdapter().list_cached_tables()
    reloj.ahora += PgSandboxAdapter._LIST_CACHE_TTL_S + 1
    tablas = await PgSandboxAdapter().list_cached_tables()

    assert listado.llamadas == 2
    assert [t.table_name for t in tablas] == ["cache_t2"]
    # Y el refresco vuelve a quedar cacheado para la instancia siguiente.
    await PgSandboxAdapter().list_cached_tables()
    assert listado.llamadas == 2


async def test_la_cache_se_separa_por_base(listado, reloj, monkeypatch) -> None:
    await PgSandboxAdapter().list_cached_tables()
    monkeypatch.setenv("SANDBOX_DATABASE_URL", "postgresql+psycopg://u:p@otra:5432/db")
    await PgSandboxAdapter().list_cached_tables()

    assert listado.llamadas == 2


async def test_mutar_el_resultado_no_ensucia_la_cache(listado, reloj) -> None:
    # `execute_sandbox_step` le agrega los marts con `.append()` a la lista
    # que recibe. Con la caché compartida, eso no puede llegar a la consulta
    # siguiente.
    tablas = await PgSandboxAdapter().list_cached_tables()
    tablas.append(CachedTableInfo(table_name="mart.x", dataset_id="", row_count=1, columns=[]))

    siguiente = await PgSandboxAdapter().list_cached_tables()
    assert [t.table_name for t in siguiente] == ["cache_t1"]


async def test_un_error_no_queda_cacheado(reloj, monkeypatch) -> None:
    llamadas = {"n": 0}

    def _falla_una_vez(_adapter):
        llamadas["n"] += 1
        if llamadas["n"] == 1:
            raise RuntimeError("se cayó la base")
        return []

    monkeypatch.setattr(PgSandboxAdapter, "_list_tables_sync", _falla_una_vez)

    with pytest.raises(RuntimeError):
        await PgSandboxAdapter().list_cached_tables()
    assert await PgSandboxAdapter().list_cached_tables() == []
    assert llamadas["n"] == 2


def test_refrescos_concurrentes_listan_una_sola_vez(monkeypatch) -> None:
    # Muchas requests con la caché vencida al mismo tiempo: una sola corre el
    # listado de 4 s; las demás esperan el lock y reusan el resultado.
    fake = _Listado(demora_s=0.2)
    monkeypatch.setattr(PgSandboxAdapter, "_list_tables_sync", lambda self: fake(self))
    key = PgSandboxAdapter()._db_url

    with ThreadPoolExecutor(max_workers=8) as pool:
        resultados = list(
            pool.map(lambda _: PgSandboxAdapter()._list_tables_cached_sync(key), range(8))
        )

    assert fake.llamadas == 1
    assert all([t.table_name for t in r] == ["cache_t1"] for r in resultados)


def test_leer_la_cache_no_espera_un_refresco_en_curso(monkeypatch) -> None:
    # La lectura corre en el event loop. Si esperara el lock del refresco
    # (tomado los ~4 s del listado), congelaría el worker entero mientras
    # otra request refresca.
    entro = threading.Event()
    soltar = threading.Event()

    def _listado_lento(_self):
        entro.set()
        soltar.wait(5)
        return []

    monkeypatch.setattr(PgSandboxAdapter, "_list_tables_sync", _listado_lento)
    key = PgSandboxAdapter()._db_url
    psa._LIST_CACHE[key] = (0.0, [])  # entrada vencida: fuerza el refresco

    hilo = threading.Thread(target=PgSandboxAdapter()._list_tables_cached_sync, args=(key,))
    hilo.start()
    try:
        assert entro.wait(5)
        t0 = time.perf_counter()
        assert psa._list_cache_get(key, PgSandboxAdapter._LIST_CACHE_TTL_S) is None
        assert time.perf_counter() - t0 < 0.5
    finally:
        soltar.set()
        hilo.join(5)
