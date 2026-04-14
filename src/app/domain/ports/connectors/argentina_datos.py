from __future__ import annotations

from abc import ABC, abstractmethod

from app.domain.entities.connectors.data_result import DataResult


class IArgentinaDatosConnector(ABC):
    @abstractmethod
    async def fetch_dolar(
        self,
        casa: str | None = None,
        ultimo: bool = False,
    ) -> DataResult | None:
        """Fetch dólar cotizaciones. If ultimo=True, returns the current spot quote."""
        ...

    @abstractmethod
    async def fetch_riesgo_pais(self, ultimo: bool = False) -> DataResult | None:
        """Fetch riesgo país (EMBI+). If ultimo=True, returns only latest."""
        ...

    @abstractmethod
    async def fetch_inflacion(self) -> DataResult | None:
        """Fetch inflación mensual."""
        ...
