"""Las herramientas del agente de respuestas (ver ``base.py``)."""

from __future__ import annotations

from typing import Any

from app.application.answers.tools.base import AgentToolImpl
from app.application.answers.tools.bcra import VariablesBCRA
from app.application.answers.tools.catalogo import (
    BuscarDatos,
    Calcular,
    DescribirTabla,
    ObtenerDatos,
)
from app.application.answers.tools.conectores import (
    BuscarSeries,
    Cotizaciones,
    DeclaracionesJuradas,
    PedirAclaracion,
    PersonalLegislativo,
    SeriesTiempo,
    Sesiones,
    UbicarLugar,
)


def build_tools(deps: Any) -> list[AgentToolImpl]:
    """Las herramientas que se pueden ofrecer con estas dependencias.

    Una dependencia que no está (sandbox, nómina) saca su herramienta en vez
    de ofrecerle al modelo algo que va a fallar. El orden es estable: la
    lista entra en el caché del prompt.
    """
    tools: list[AgentToolImpl] = [BuscarSeries(), SeriesTiempo()]
    if getattr(deps, "bcra", None) is not None:
        tools.append(VariablesBCRA())
    if deps.sandbox is not None:
        tools += [BuscarDatos(), DescribirTabla(), ObtenerDatos(), Calcular()]
    if deps.arg_datos is not None:
        tools.append(Cotizaciones())
    if deps.ddjj is not None:
        tools.append(DeclaracionesJuradas())
    if deps.sesiones is not None:
        tools.append(Sesiones())
    if deps.staff is not None:
        tools.append(PersonalLegislativo())
    if deps.georef is not None:
        tools.append(UbicarLugar())
    tools.append(PedirAclaracion())
    return tools
