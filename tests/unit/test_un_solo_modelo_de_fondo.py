"""Las tareas de fondo usan el mismo modelo que el resto del backend.

`analyst_tasks` y `catalog_enrichment_tasks` leían `BEDROCK_MODEL_ID` con
su propio default —**Claude 3.5 Haiku, de octubre de 2024**— mientras el
resto del sistema corría con `BEDROCK_LLM_MODEL`, que en staging y prod
apunta a Haiku 4.5. Una generación entera de diferencia.

No era una decisión: son dos literales copiados, sin un comentario que
los justificara, y `BEDROCK_MODEL_ID` no está seteada en ningún entorno —
o sea que nadie eligió ese modelo, simplemente quedó.

Importa porque el enriquecimiento de catálogo genera la metadata sobre la
que después se hacen las búsquedas, y quedan 29.587 recursos por enriquecer.
"""

from __future__ import annotations

import pytest

from app.setup.config.constants import BEDROCK_LLM_MODEL_DEFAULT, bedrock_llm_model


@pytest.fixture(autouse=True)
def _sin_modelos_en_el_entorno(monkeypatch):
    for v in ("BEDROCK_MODEL_ID", "BEDROCK_LLM_MODEL"):
        monkeypatch.delenv(v, raising=False)


def test_hereda_el_modelo_del_sistema(monkeypatch):
    """El caso real: `BEDROCK_LLM_MODEL` está seteada, `BEDROCK_MODEL_ID` no."""
    monkeypatch.setenv("BEDROCK_LLM_MODEL", "us.anthropic.claude-haiku-4-5-20251001-v1:0")

    assert bedrock_llm_model() == "us.anthropic.claude-haiku-4-5-20251001-v1:0"


def test_un_override_explicito_sigue_mandando(monkeypatch):
    """Quien tenga `BEDROCK_MODEL_ID` seteada no se ve afectado."""
    monkeypatch.setenv("BEDROCK_LLM_MODEL", "modelo-del-sistema")
    monkeypatch.setenv("BEDROCK_MODEL_ID", "modelo-elegido-a-proposito")

    assert bedrock_llm_model() == "modelo-elegido-a-proposito"


def test_sin_nada_configurado_cae_al_default():
    assert bedrock_llm_model() == BEDROCK_LLM_MODEL_DEFAULT


def test_el_default_coincide_con_el_de_settings():
    """Las dos definiciones vivían separadas y se distanciaron una generación.

    Este test las ata: si alguien actualiza el modelo en `settings.py` y se
    olvida de acá, vuelve a haber dos modelos conviviendo sin que nadie lo
    haya decidido — que es exactamente cómo se llegó a esto.
    """
    from app.setup.config.settings import BedrockSettings

    assert BEDROCK_LLM_MODEL_DEFAULT == BedrockSettings.model_fields["LLM_MODEL"].default


def test_ninguna_tarea_tiene_su_propio_literal():
    """Un default nuevo copiado a mano reabre la divergencia."""
    from pathlib import Path

    for archivo in ("analyst_tasks.py", "catalog_enrichment_tasks.py"):
        fuente = Path(f"src/app/infrastructure/celery/tasks/{archivo}").read_text(encoding="utf-8")
        assert "claude-3-5-haiku" not in fuente, f"{archivo} volvió a fijar Haiku 3.5"
        assert 'os.getenv("BEDROCK_MODEL_ID"' not in fuente, (
            f"{archivo} lee la variable directo en vez de usar `bedrock_llm_model()`"
        )
