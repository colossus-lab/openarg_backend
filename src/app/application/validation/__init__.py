"""Ingestion validation (WS0).

Componente arquitectónico transversal que valida cada recurso ingresado
en tres momentos del ciclo de vida (pre-parse, post-parse, retrospective)
y persiste findings auditables en `ingestion_findings`.

Ver collector_plan.md WS0 para diseño completo.
"""

from app.application.validation.detector import (
    Detector,
    Finding,
    Mode,
    ResourceContext,
    Severity,
)
from app.application.validation.ingestion_validator import IngestionValidator

__all__ = [
    "Detector",
    "Finding",
    "IngestionValidator",
    "Mode",
    "ResourceContext",
    "Severity",
]
