from __future__ import annotations

import json
import logging

from app.domain.entities.connectors.data_result import DataResult, ExecutionPlan, MemoryContext
from app.domain.ports.cache.cache_port import ICacheService
from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage
from app.prompts import load_prompt

logger = logging.getLogger(__name__)

MEMORY_TTL = 30 * 60  # 30 minutes


def _memory_cache_key(session_id: str) -> str:
    return f"openarg:memory:{session_id}"


async def load_memory(cache: ICacheService, session_id: str) -> MemoryContext:
    """Load memory context from Redis for a session."""
    if not session_id:
        return MemoryContext()
    try:
        data = await cache.get(_memory_cache_key(session_id))
        if data and isinstance(data, dict):
            return MemoryContext(
                turn_number=data.get("turn_number", 0),
                summaries=data.get("summaries", []),
                key_findings=data.get("key_findings", []),
                datasets_used=data.get("datasets_used", []),
                pending_questions=data.get("pending_questions", []),
            )
    except Exception:
        logger.debug("Failed to load memory for session %s", session_id)
    return MemoryContext()


async def save_memory(cache: ICacheService, session_id: str, memory: MemoryContext) -> None:
    """Save memory context to Redis."""
    if not session_id:
        return
    try:
        data = {
            "turn_number": memory.turn_number,
            "summaries": memory.summaries,
            "key_findings": memory.key_findings,
            "datasets_used": memory.datasets_used,
            "pending_questions": memory.pending_questions,
        }
        await cache.set(_memory_cache_key(session_id), data, ttl_seconds=MEMORY_TTL)
    except Exception:
        logger.debug("Failed to save memory for session %s", session_id)


async def update_memory(
    llm: ILLMProvider,
    current_memory: MemoryContext,
    plan: ExecutionPlan,
    results: list[DataResult],
    analysis_text: str,
) -> MemoryContext:
    """Update memory after a completed analysis turn."""
    try:
        context = (
            f"TURNO {current_memory.turn_number + 1}\n\n"
            f'PREGUNTA: "{plan.query}"\n'
            f"INTENCIÓN: {plan.intent}\n\n"
            f"DATOS RECOLECTADOS: {len(results)} datasets de: "
            f"{', '.join(r.portal_name for r in results)}\n\n"
            f"ANÁLISIS PRODUCIDO (resumen):\n{analysis_text[:1500]}\n\n"
            f"CONTEXTO PREVIO:\n"
        )
        if current_memory.summaries:
            context += "\n".join(current_memory.summaries[-3:])
        else:
            context += "Primera interacción"

        context += "\n\nGenerá el resumen de memoria en JSON."

        response = await llm.chat(
            messages=[
                LLMMessage(role="system", content=load_prompt("memory")),
                LLMMessage(role="user", content=context),
            ],
            temperature=0.3,
            max_tokens=1024,
        )

        parsed = json.loads(response.content.strip())

        new_datasets = parsed.get("datasetsUsed", [])
        all_datasets = list(set(current_memory.datasets_used + new_datasets))

        return MemoryContext(
            turn_number=current_memory.turn_number + 1,
            summaries=(current_memory.summaries + [parsed.get("summary", "")])[-10:],
            key_findings=(current_memory.key_findings + parsed.get("keyFindings", []))[-20:],
            datasets_used=all_datasets,
            pending_questions=parsed.get("suggestedFollowups", []),
        )
    except Exception:
        logger.debug("LLM memory update failed, using simple fallback", exc_info=True)
        # Simple fallback without LLM
        new_datasets = [r.dataset_title for r in results]
        all_datasets = list(set(current_memory.datasets_used + new_datasets))

        return MemoryContext(
            turn_number=current_memory.turn_number + 1,
            summaries=(
                current_memory.summaries
                + [f"Turno {current_memory.turn_number + 1}: {plan.intent}"]
            )[-10:],
            key_findings=current_memory.key_findings,
            datasets_used=all_datasets,
            pending_questions=[],
        )


def build_memory_context_prompt(memory: MemoryContext) -> str:
    """Build a prompt section with memory context for the planner/analysis."""
    if memory.turn_number == 0:
        return ""

    parts = [f"\nCONTEXTO DE CONVERSACIÓN (turno {memory.turn_number + 1}):"]

    if memory.summaries:
        parts.append("Turnos previos:")
        for s in memory.summaries[-3:]:
            parts.append(f"  - {s}")

    if memory.key_findings:
        parts.append("Hallazgos clave previos:")
        for f in memory.key_findings[-5:]:
            parts.append(f"  - {f}")

    if memory.datasets_used:
        parts.append(f"Datasets ya consultados: {', '.join(memory.datasets_used[-10:])}")

    if memory.pending_questions:
        parts.append("Preguntas sugeridas pendientes:")
        for q in memory.pending_questions:
            parts.append(f"  - {q}")

    parts.append(
        "INSTRUCCIÓN: Usá este contexto para dar continuidad a la conversación. "
        "Si el usuario hace una pregunta de seguimiento, conectá con lo anterior. "
        "Evitá repetir datasets ya consultados si no es necesario.\n"
    )
    return "\n".join(parts)
