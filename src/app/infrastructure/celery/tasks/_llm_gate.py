"""The model, but only if it can still answer something we know the answer to.

Two places now want a model for a job where a wrong answer looks exactly like a
right one: renaming a column, and mapping a source field. Both need the same
precondition and neither should own it, so it lives here.

A degraded model — a version change, a throttled endpoint answering with a stub,
a prompt that stopped fitting — does not produce obvious nonsense. It produces
confident, well-formed, plausible answers about the wrong thing, and every
structural check downstream passes. The canary is the only gate that catches
that, because it is the only one that knows what the answer should be.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def model_if_it_answers() -> tuple[Any, str]:
    """`(adapter, detail)`, or `(None, why)` when the model should not be used.

    Callers carry on without it rather than aborting: the deterministic tiers
    are still worth running, and a missing model is a reason to do less, not a
    reason to do nothing.
    """
    import asyncio

    try:
        from app.application.quality.model_canary import run_canary
        from app.application.repair.parse_repair import propose_llm_assisted_rename
        from app.infrastructure.adapters.llm.bedrock_llm_adapter import BedrockLLMAdapter

        adapter = BedrockLLMAdapter()
        canary = asyncio.run(run_canary(adapter, propose_llm_assisted_rename))
    except Exception as exc:
        logger.warning("llm gate: canary unavailable", exc_info=True)
        return None, f"no disponible: {type(exc).__name__}"

    if not canary.ok:
        logger.warning("llm gate: canary failed — %s", canary.detail)
        return None, f"falló: {canary.detail}"
    return adapter, canary.detail
