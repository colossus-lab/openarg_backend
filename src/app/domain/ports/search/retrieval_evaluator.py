"""Port for CRAG-style retrieval quality evaluation."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class RetrievalQuality(Enum):
    CORRECT = "correct"
    AMBIGUOUS = "ambiguous"
    INCORRECT = "incorrect"


@dataclass
class RetrievalVerdict:
    quality: RetrievalQuality
    confidence: float  # 0.0 - 1.0
    reasoning: str = ""
    suggested_actions: list[str] = field(default_factory=list)


class IRetrievalEvaluator(ABC):
    @abstractmethod
    async def evaluate(
        self,
        question: str,
        results: list[Any],
        plan: Any,
    ) -> RetrievalVerdict: ...
