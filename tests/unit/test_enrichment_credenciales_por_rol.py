"""El enriquecimiento del catálogo corre con credenciales por rol de la instancia.

Antes miraba sólo `AWS_ACCESS_KEY_ID`: con el rol de EC2 y sin clave en el
`.env`, las dos tareas se salteaban en silencio.
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.infrastructure.celery.tasks import catalog_enrichment_tasks as tasks


class _FakeEngine:
    def __init__(self) -> None:
        self.disposed = False

    def dispose(self) -> None:
        self.disposed = True


def _session_with(credentials):
    return lambda: SimpleNamespace(get_credentials=lambda: credentials)


@pytest.fixture
def _sin_clave_en_el_entorno(monkeypatch):
    monkeypatch.delenv("AWS_ACCESS_KEY_ID", raising=False)
    monkeypatch.delenv("AWS_SECRET_ACCESS_KEY", raising=False)


@pytest.mark.usefixtures("_sin_clave_en_el_entorno")
def test_con_rol_de_la_instancia_y_sin_clave_en_el_entorno_enriquece(monkeypatch):
    engine = _FakeEngine()
    monkeypatch.setattr(tasks.boto3, "Session", _session_with(SimpleNamespace(method="iam-role")))
    monkeypatch.setattr(tasks, "get_sync_engine", lambda: engine)
    llamadas = []
    monkeypatch.setattr(tasks, "_enrich_table", lambda eng, name: llamadas.append(name) or True)

    result = tasks.enrich_single_table.run("cache_ejemplo")

    assert result == {"table_name": "cache_ejemplo", "enriched": True}
    assert llamadas == ["cache_ejemplo"]
    assert engine.disposed


@pytest.mark.usefixtures("_sin_clave_en_el_entorno")
def test_sin_ninguna_credencial_se_saltea_como_antes(monkeypatch):
    monkeypatch.setattr(tasks.boto3, "Session", _session_with(None))
    monkeypatch.setattr(tasks, "get_sync_engine", lambda: pytest.fail("no debería abrir la base"))

    assert tasks.enrich_single_table.run("cache_ejemplo") == {"error": "no_aws_credentials"}
    assert tasks.enrich_all_tables.run() == {"error": "no_aws_credentials"}


def test_si_boto3_falla_al_resolver_credenciales_no_revienta(monkeypatch):
    def _explota():
        raise RuntimeError("metadata de la instancia inaccesible")

    monkeypatch.setattr(tasks.boto3, "Session", _explota)

    assert tasks._aws_credentials_available() is False
