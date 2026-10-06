"""El prompt del agente, armado por el mismo cargador que usa el motor.

Las reglas del 04-oct salen de la auditoría verificada: el modelo sumaba tasas
de cabeza («≈14 %» por 14,58 %), presentaba un dato de abril como "actual",
atribuía causas ante "relación entre X e Y" y no sabía qué temas no cubre
OpenArg.
"""

from __future__ import annotations

from datetime import date

from app.application.answers.prompt import BCRA_TOOL, system_prompt


def test_las_reglas_nuevas_llegan_por_el_cargador() -> None:
    prompt = system_prompt(date(2026, 10, 4))
    # Cuentas.
    assert "No hagas cuentas" in prompt
    assert "Las tasas no se suman ni se restan" in prompt
    assert "la interanual no es la resta de dos mensuales" in prompt
    assert "No expliques por qué ni menciones estas reglas" in prompt
    # Frescura.
    assert "Decí siempre de cuándo es el dato" in prompt
    assert 'Nunca uses "actual", "actualmente", "hoy", "reciente"' in prompt
    assert "promedio mensual como el valor de un día" in prompt
    # Neutralidad: causas, políticas y personas.
    assert "No atribuyas causas, no evalúes políticas, gestiones, gobiernos ni personas" in prompt
    assert "citado y atribuido" in prompt
    # Cobertura: lo que falta, y que licitaciones sí hay.
    assert "coparticipación federal" in prompt
    assert "ENRE" in prompt
    assert "stock de deuda pública nacional" in prompt
    assert "Compras, contrataciones y licitaciones sí hay" in prompt
    assert prompt.endswith("Hoy es 2026-10-04.")


def test_superlativos_comparaciones_y_rangos_solo_si_los_calcula_una_herramienta() -> None:
    """Revisión del 05-oct (C6/H006 y A7/H016): «31,6 % es el nivel más bajo
    desde 2016» con 25,7 % en la evidencia del mismo turno, y «se estabilizó
    en 1,7-2,1 % durante 2025» con octubre-diciembre en 2,34/2,47/2,85. El
    verificador compara cifras, no superlativos ni rangos."""
    prompt = system_prompt(date(2026, 10, 6))
    assert "superlativos" in prompt
    assert '"el más bajo desde 2016"' in prompt
    assert '"récord"' in prompt
    assert '"entre X e Y durante 2025"' in prompt
    assert "que no haya calculado una herramienta" in prompt
    assert "Si la persona los pide, calculalos con una herramienta" in prompt


def test_la_regla_del_bcra_va_solo_si_esta_la_herramienta() -> None:
    sin = system_prompt(date(2026, 10, 4), tool_names={"buscar_series", "series_tiempo"})
    con = system_prompt(date(2026, 10, 4), tool_names={"buscar_series", BCRA_TOOL})
    assert BCRA_TOOL not in sin
    assert system_prompt(date(2026, 10, 4)) == sin
    assert f"usá primero {BCRA_TOOL}" in con
    assert "DolarApi y ArgentinaDatos no son oficiales" in con


def test_el_prompt_no_tiene_llaves_sueltas_del_formato() -> None:
    """El texto pasa por ``str.format``: una llave sin escapar lo rompería."""
    prompt = system_prompt(date(2026, 10, 4), tool_names={BCRA_TOOL})
    assert "{" not in prompt and "}" not in prompt
