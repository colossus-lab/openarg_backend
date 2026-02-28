from __future__ import annotations

from app.domain.ports.connectors.argentina_datos import IArgentinaDatosConnector
from app.domain.ports.connectors.ckan_search import ICKANSearchConnector
from app.domain.ports.connectors.georef import IGeorefConnector
from app.domain.ports.connectors.series_tiempo import ISeriesTiempoConnector
from app.domain.ports.connectors.sesiones import ISesionesConnector

__all__ = [
    "IArgentinaDatosConnector",
    "ICKANSearchConnector",
    "IGeorefConnector",
    "ISeriesTiempoConnector",
    "ISesionesConnector",
]
