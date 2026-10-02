"""El modelo de un agente que usa herramientas.

``ILLMProvider`` habla en texto: un prompt entra, un texto sale. Un agente
necesita más: mensajes por bloques (texto, pedido de herramienta, resultado
de herramienta), la definición de las herramientas, el motivo de corte
(¿terminó o pidió una herramienta?) y el uso de tokens con caché, para que el
costo de un turno se pueda medir entero.

Los mensajes usan la forma de la API de mensajes de Anthropic
(``{"role": ..., "content": [bloques]}``): es la que habla el modelo, y
traducirla a otra para volver a traducirla en el adapter no agrega nada.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class AgentTool:
    name: str
    description: str
    input_schema: dict[str, Any]


@dataclass(frozen=True)
class ToolCall:
    id: str
    name: str
    input: dict[str, Any]


@dataclass
class AgentUsage:
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0

    @property
    def total(self) -> int:
        return (
            self.input_tokens
            + self.output_tokens
            + self.cache_read_tokens
            + self.cache_write_tokens
        )

    def add(self, other: AgentUsage) -> None:
        self.input_tokens += other.input_tokens
        self.output_tokens += other.output_tokens
        self.cache_read_tokens += other.cache_read_tokens
        self.cache_write_tokens += other.cache_write_tokens


@dataclass(frozen=True)
class TextDelta:
    """Un fragmento de texto, a medida que el modelo lo escribe."""

    text: str


@dataclass
class AgentTurn:
    """Una respuesta completa del modelo: lo que dijo y qué herramientas pidió."""

    text: str
    tool_calls: list[ToolCall]
    # "end_turn", "tool_use", "max_tokens", "refusal"…
    stop_reason: str
    usage: AgentUsage
    # Los bloques tal como hay que devolverlos en el turno siguiente.
    content: list[dict[str, Any]] = field(default_factory=list)


class IAgentLLM(ABC):
    @property
    @abstractmethod
    def model(self) -> str: ...

    @abstractmethod
    def stream_turn(
        self,
        *,
        system: str,
        messages: list[dict[str, Any]],
        tools: list[AgentTool],
        max_tokens: int = 4096,
        allow_tools: bool = True,
    ) -> AsyncGenerator[TextDelta | AgentTurn, None]:
        """Una vuelta del modelo: emite ``TextDelta`` y termina con un ``AgentTurn``.

        ``allow_tools=False`` le pide la respuesta sin más herramientas. Las
        definiciones se siguen mandando: la conversación ya tiene pedidos y
        resultados de herramientas, y sin ellas el pedido no es válido.
        """
        ...
