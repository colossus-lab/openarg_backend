"""Cuánto cuesta un turno, con el modelo que lo contestó.

Antes había un costo fijo por respuesta (US$ 0,034, medido en CloudWatch con
Haiku). Con dos modelos posibles y un número variable de vueltas por turno,
el costo sale de los tokens reales.

Precios de lista de Anthropic por millón de tokens. Bedrock factura aparte y
puede diferir: sirve para comparar motores y modelos, no para conciliar la
factura de AWS.
"""

from __future__ import annotations

from app.domain.ports.llm.agent_llm import AgentUsage

# (entrada, salida, escritura de caché, lectura de caché), por millón.
PRICES_PER_MTOK: dict[str, tuple[float, float, float, float]] = {
    "claude-haiku-4-5": (1.00, 5.00, 1.25, 0.10),
    "claude-sonnet-4-6": (3.00, 15.00, 3.75, 0.30),
}


def cost_usd(model: str, usage: AgentUsage) -> float | None:
    """Costo en USD, o None si el modelo no tiene precio cargado."""
    price = next((p for key, p in PRICES_PER_MTOK.items() if key in (model or "")), None)
    if price is None:
        return None
    p_in, p_out, p_write, p_read = price
    total = (
        usage.input_tokens * p_in
        + usage.output_tokens * p_out
        + usage.cache_write_tokens * p_write
        + usage.cache_read_tokens * p_read
    ) / 1_000_000
    return round(total, 6)
