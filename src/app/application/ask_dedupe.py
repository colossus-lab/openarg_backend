"""La misma pregunta repetida enseguida no se vuelve a correr ni a cobrar (``/ask``).

Lo que lo motivó (01 al 04-oct-2026): una integración n8n mandaba la MISMA
pregunta cada ~21 s, el patrón de un cliente con timeout de ~20 s y tres
intentos. El motor tardaba más que eso, así que cada intento cortaba, se
cobraba igual y el siguiente volvía a correr el motor desde cero: del 01 al
03-oct probablemente no recibió ninguna respuesta y gastó 2 preguntas por día.

Ahora, por clave y pregunta normalizada:

- si hay una respuesta de los últimos 5 minutos, se devuelve esa: sin cobrar,
  sin correr el motor y sin contar el límite por minuto;
- si la misma pregunta está corriendo, se espera a que termine y se devuelve
  la misma respuesta, sin cobrar;
- si la que corría falló (timeout, error), no hay nada guardado y el pedido
  siguiente la vuelve a correr: un error no se repite.

La coordinación va en Redis, así funciona entre los workers de uvicorn:

- ``ask:dedupe:{huella}:answer``: la respuesta (sin el cupo), 5 minutos;
- ``ask:dedupe:{huella}:lock``: el turno en curso. Se toma con el mismo INCR
  atómico de los contadores (el pedido que recibe 1 es el que corre) y vence
  solo si el proceso muere a mitad de camino.

Si Redis no responde no hay deduplicación: cada pedido corre como antes.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import unicodedata
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from app.domain.ports.cache.cache_port import ICacheService

logger = logging.getLogger(__name__)

# Cuánto vale una respuesta ya calculada para la misma clave y pregunta.
ANSWER_TTL_SECONDS = 300
_POLL_SECONDS = 0.5
# Lo que sobrevive el candado al tope del turno, por si el proceso que corre
# muere sin soltarlo. Cubre lo que pasa después del motor (cobrar, registrar,
# guardar la respuesta); si se vence antes, lo peor es una corrida de más.
LOCK_MARGIN_SECONDS = 15

# Signos que no cambian la pregunta: "¿Desempleo 2024?" y "desempleo 2024".
_EDGE_CHARS = " \t\r\n¿?¡!.,;:"


def normalize_question(question: str) -> str:
    """La pregunta para comparar: mismas letras, sin mayúsculas ni espacios de más.

    Conservadora a propósito: no saca acentos ni palabras. Lo que se busca es
    el reintento automático de la misma pregunta, no parecidos.
    """
    text = unicodedata.normalize("NFKC", question).casefold()
    return " ".join(text.split()).strip(_EDGE_CHARS)


def question_fingerprint(api_key_id: object, question: str) -> str:
    """Huella de clave + pregunta normalizada. La pregunta no queda en Redis."""
    raw = f"{api_key_id}\n{normalize_question(question)}"
    return hashlib.sha256(raw.encode()).hexdigest()[:40]


def _answer_key(fingerprint: str) -> str:
    return f"ask:dedupe:{fingerprint}:answer"


def _lock_key(fingerprint: str) -> str:
    return f"ask:dedupe:{fingerprint}:lock"


async def cached_answer(cache: ICacheService, fingerprint: str) -> dict[str, Any] | None:
    """La respuesta guardada para esta huella, si hay."""
    try:
        value = await cache.get(_answer_key(fingerprint))
    except Exception:
        logger.warning("Ask dedupe lookup failed; running the question", exc_info=True)
        return None
    return value if isinstance(value, dict) else None


async def store_answer(cache: ICacheService, fingerprint: str, payload: dict[str, Any]) -> None:
    """Guarda una respuesta completa (200) para los reintentos de los próximos minutos."""
    try:
        await cache.set(_answer_key(fingerprint), payload, ttl_seconds=ANSWER_TTL_SECONDS)
    except Exception:
        logger.warning("Ask dedupe store failed", exc_info=True)


async def release(cache: ICacheService, fingerprint: str) -> None:
    """Suelta el turno. Va siempre después de guardar la respuesta (si la hubo)."""
    try:
        await cache.delete(_lock_key(fingerprint))
    except Exception:
        logger.warning("Ask dedupe lock release failed; it expires on its own", exc_info=True)


@dataclass(frozen=True)
class Turn:
    """Qué le toca a un pedido: correr la pregunta, o la respuesta de otro."""

    leader: bool
    answer: dict[str, Any] | None = None

    @property
    def gave_up(self) -> bool:
        """Esperó más que el tope de un turno y la otra corrida no terminó."""
        return not self.leader and self.answer is None


async def lead_or_wait(
    cache: ICacheService,
    fingerprint: str,
    *,
    lock_ttl: int,
    wait_s: float,
    poll_s: float = _POLL_SECONDS,
) -> Turn:
    """Toma el turno de esta pregunta o espera la respuesta del que lo tiene.

    Si el que corría suelta el turno sin respuesta (falló), lo toma el primero
    que llegue y vuelve a correr la pregunta.
    """
    loop = asyncio.get_running_loop()
    give_up_at = loop.time() + wait_s
    lock = _lock_key(fingerprint)
    while True:
        try:
            first = await cache.increment_with_ttl(lock, ttl_seconds=lock_ttl) == 1
        except Exception:
            logger.warning("Ask dedupe lock unavailable; running the question", exc_info=True)
            return Turn(leader=True)
        if first:
            # El anterior pudo guardar y soltar entre la lectura y el INCR.
            answer = await cached_answer(cache, fingerprint)
            if answer is not None:
                await release(cache, fingerprint)
                return Turn(leader=False, answer=answer)
            return Turn(leader=True)
        answer = await cached_answer(cache, fingerprint)
        if answer is not None:
            return Turn(leader=False, answer=answer)
        if loop.time() >= give_up_at:
            return Turn(leader=False)
        await asyncio.sleep(poll_s)
