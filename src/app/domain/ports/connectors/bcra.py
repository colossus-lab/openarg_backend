from __future__ import annotations

from abc import ABC, abstractmethod

from app.domain.entities.connectors.data_result import DataResult


class IBCRAConnector(ABC):
    """API pública del BCRA: estadísticas cambiarias v1 y monetarias v4."""

    @abstractmethod
    async def get_cotizaciones(
        self,
        moneda: str | None = None,
        fecha_desde: str | None = None,
        fecha_hasta: str | None = None,
    ) -> DataResult:
        """Cotizaciones de un día (todas las monedas) o la historia de una moneda.

        Cada registro trae ``fecha``: la del día publicado, no la de la consulta.
        """
        ...

    @abstractmethod
    async def get_cotizaciones_historicas(self, moneda: str, desde: str, hasta: str) -> DataResult:
        """Historia diaria de una moneda entre ``desde`` y ``hasta`` (AAAA-MM-DD)."""
        ...

    @abstractmethod
    async def list_variables(self) -> list[dict]:
        """El catálogo de variables monetarias v4 (id, descripción, unidad, última fecha)."""
        ...

    @abstractmethod
    async def get_variable(
        self,
        id_variable: int,
        desde: str | None = None,
        hasta: str | None = None,
        *,
        limit: int | None = None,
        title: str | None = None,
        plazo_s: float | None = None,
    ) -> DataResult:
        """Observaciones de una variable monetaria v4, en orden cronológico.

        Con ``plazo_s``, si no termina en ese tiempo es un ConnectorError (y una
        falla de la fuente), no una espera que corte el que llama.
        """
        ...
