"""Skill registry — automatic detection and context injection for the pipeline.

Skills are detected via regex patterns on the preprocessed query.
When a skill matches, its ``planner_injection`` and ``analyst_injection``
are appended to the respective prompts, enriching the pipeline with
domain-specific instructions without adding LLM cost.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field


@dataclass(frozen=True)
class SkillDef:
    """Definition of a single skill with detection patterns and prompt injections."""

    name: str
    description: str
    planner_injection: str
    analyst_injection: str
    patterns: tuple[re.Pattern[str], ...] = field(default_factory=tuple)


class SkillRegistry:
    """Registry of built-in skills with automatic detection."""

    def __init__(self) -> None:
        self._skills: dict[str, SkillDef] = {}
        for skill in _BUILTIN_SKILLS:
            self._skills[skill.name] = skill

    def match_auto(self, question: str) -> SkillDef | None:
        """Return the first skill whose pattern matches the question, or None."""
        q = question.lower().strip()
        for skill in self._skills.values():
            for pattern in skill.patterns:
                if pattern.search(q):
                    return skill
        return None

    def list_all(self) -> list[SkillDef]:
        """Return all registered skills."""
        return list(self._skills.values())


# ---------------------------------------------------------------------------
# Built-in skills
# ---------------------------------------------------------------------------

_I = re.IGNORECASE

_SKILL_VERIFICAR = SkillDef(
    name="verificar",
    description="Verificación de afirmaciones con datos oficiales",
    patterns=(
        re.compile(r"es\s+(?:verdad|cierto|falso|mentira)\s+que", _I),
        re.compile(r"(?:dijo|afirm[oó]|declar[oó]|asegur[oó])\s+que", _I),
        re.compile(r"(?:verificar?|chequear?|confirmar?|fact.?check)", _I),
        re.compile(
            r"(?:aument[oó]|baj[oó]|subi[oó]|creci[oó]|cay[oó]|redujo)\s+(?:un\s+)?\d+\s*%",
            _I,
        ),
    ),
    planner_injection=(
        "SKILL ACTIVA: VERIFICACIÓN DE AFIRMACIÓN\n"
        "El usuario quiere verificar si una afirmación es verdadera o falsa.\n"
        "Tu plan DEBE incluir:\n"
        "1. Los datos necesarios para CONFIRMAR o REFUTAR la afirmación.\n"
        "2. Si la afirmación menciona un porcentaje de aumento/baja, DEBES incluir "
        "query_series con la serie de inflación (148.3_INIVELNAL_DICI_M_26) para "
        "poder calcular la variación en términos REALES (ajustados por inflación).\n"
        "3. Si la afirmación es sobre presupuesto/gasto, DEBES incluir query_sandbox "
        "con tables: [\"cache_presupuesto_*\"] para obtener crédito vigente y devengado.\n"
        "4. Buscá datos que puedan CONTRADECIR la afirmación, no solo confirmarla.\n"
        "IMPORTANTE: Generá al menos 2 pasos de recolección de datos."
    ),
    analyst_injection=(
        "FORMATO REQUERIDO — VERIFICACIÓN DE AFIRMACIÓN:\n"
        "Respondé con este formato estricto:\n\n"
        "**Afirmación**: [repetir la afirmación del usuario]\n\n"
        "**Datos encontrados**: [resumen de los datos recolectados con cifras exactas]\n\n"
        "**Veredicto**: [UNO de: VERDADERO / ENGAÑOSO / FALSO / SIN DATOS SUFICIENTES]\n"
        "- VERDADERO: los datos confirman la afirmación tal como fue dicha\n"
        "- ENGAÑOSO: la afirmación es técnicamente correcta pero omite contexto crucial "
        "(ej: aumento nominal pero caída en términos reales)\n"
        "- FALSO: los datos contradicen la afirmación\n"
        "- SIN DATOS SUFICIENTES: no hay datos disponibles para verificar\n\n"
        "**Contexto**: [1-2 oraciones de contexto adicional]\n\n"
        "Si hay datos temporales, incluí un gráfico con <!--CHART:{}-->.\n"
        "IMPORTANTE: Siempre distinguí entre variación NOMINAL y REAL (ajustada por inflación)."
    ),
)

_SKILL_COMPARAR = SkillDef(
    name="comparar",
    description="Comparativa estructurada entre dos o más elementos",
    patterns=(
        re.compile(r"\bvs\.?\b|\bversus\b", _I),
        re.compile(r"compar(?:ar|[aá]|aci[oó]n|ativo)\s+", _I),
        re.compile(r"(?:diferencia|contraste)\s+entre", _I),
    ),
    planner_injection=(
        "SKILL ACTIVA: COMPARACIÓN\n"
        "El usuario quiere comparar dos o más elementos.\n"
        "Tu plan DEBE:\n"
        "1. Generar pasos PARALELOS (sin dependencias entre sí) para cada lado "
        "de la comparación.\n"
        "2. Si son personas (patrimonio, DDJJ) → generar UN query_ddjj por persona "
        "con action 'search' y el nombre como parámetro.\n"
        "3. Si son indicadores económicos → generar query_series paralelos.\n"
        "4. Si son áreas presupuestarias → generar query_sandbox paralelos con "
        "tables: [\"cache_presupuesto_*\"].\n"
        "5. Los pasos deben ser independientes para ejecutarse en paralelo.\n"
        "IMPORTANTE: NUNCA pongas ambos lados en un solo paso."
    ),
    analyst_injection=(
        "FORMATO REQUERIDO — COMPARACIÓN:\n"
        "Respondé con este formato:\n\n"
        "1. **Tabla comparativa** con las métricas clave lado a lado.\n"
        "2. **Diferencias principales**: 2-3 puntos destacados.\n"
        "3. **Gráfico comparativo**: incluí SIEMPRE un <!--CHART:{\"type\":\"bar_chart\",...}--> "
        "comparando los valores principales.\n\n"
        "Usá formato de tabla markdown:\n"
        "| Métrica | [A] | [B] |\n"
        "|---------|-----|-----|\n"
        "| ... | ... | ... |"
    ),
)

_SKILL_PRESUPUESTO = SkillDef(
    name="presupuesto",
    description="Análisis presupuestario detallado con ejecución y desglose",
    patterns=(
        re.compile(r"presupuesto\s+(?:de(?:l)?|para|en)\s+", _I),
        re.compile(r"(?:gasto|ejecuci[oó]n|cr[eé]dito)\s+(?:p[uú]blico|presupuestar)", _I),
        re.compile(
            r"cu[aá]nto\s+(?:se\s+)?(?:gast[oó]|invirti[oó]|ejecut[oó]|destin[oó])",
            _I,
        ),
        re.compile(r"ejecuci[oó]n\s+presupuestaria", _I),
    ),
    planner_injection=(
        "SKILL ACTIVA: ANÁLISIS PRESUPUESTARIO\n"
        "El usuario quiere un análisis detallado de presupuesto.\n"
        "Tu plan DEBE incluir:\n"
        "1. query_sandbox con tables: [\"cache_presupuesto_*\"] para obtener "
        "crédito original, vigente Y devengado.\n"
        "2. query_sandbox con tables: [\"cache_presupuesto_dim_apertura_programatica_*\"] "
        "para desglose por programa/subprograma.\n"
        "3. Si hay comparación interanual, incluir query_series con la serie de "
        "inflación (148.3_INIVELNAL_DICI_M_26) para deflactar y mostrar variación real.\n"
        "IMPORTANTE: Siempre incluí crédito vigente Y devengado, no solo uno."
    ),
    analyst_injection=(
        "FORMATO REQUERIDO — ANÁLISIS PRESUPUESTARIO:\n"
        "Respondé con este formato:\n\n"
        "**Resumen ejecutivo**: 2-3 oraciones con las cifras principales.\n\n"
        "**Ejecución presupuestaria**:\n"
        "- Crédito original: $X\n"
        "- Crédito vigente: $Y (variación: Z%)\n"
        "- Devengado: $W (ejecución: W/Y %)\n\n"
        "**Desglose por programa** (si hay datos): listar los 5 programas principales "
        "con montos.\n\n"
        "**Variación interanual** (si hay datos de años anteriores): "
        "mostrar variación nominal Y real (ajustada por inflación).\n\n"
        "Incluí un gráfico con <!--CHART:{}-->.\n"
        "Formateá montos en pesos argentinos legibles (ej: $1.234.567.890)."
    ),
)

_SKILL_PERFIL = SkillDef(
    name="perfil",
    description="Perfil completo de legislador con patrimonio y personal",
    patterns=(
        re.compile(
            r"(?:qui[eé]n\s+es|datos\s+de|info(?:rmaci[oó]n)?\s+(?:de|sobre))"
            r"\s+(?:(?:el|la)\s+)?(?:diputad|senad|legislad)",
            _I,
        ),
        re.compile(r"(?:patrimonio|bienes|ddjj|declaraci[oó]n)\s+(?:de|del)\s+\w+", _I),
        re.compile(
            r"(?:asesores?|empleados?|personal)\s+de(?:l)?\s+(?:diputad|senad|legislad)",
            _I,
        ),
    ),
    planner_injection=(
        "SKILL ACTIVA: PERFIL DE LEGISLADOR\n"
        "El usuario quiere un perfil completo de un legislador.\n"
        "Tu plan DEBE incluir estos dos pasos OBLIGATORIOS en paralelo:\n"
        "1. query_ddjj con action 'search' o 'detail' y el nombre del legislador — "
        "para obtener patrimonio declarado, bienes, ingresos.\n"
        "2. query_staff con action 'get_by_legislator' y el nombre — "
        "para obtener personal a cargo.\n"
        "Ambos pasos deben ejecutarse en PARALELO (sin depends_on entre sí).\n"
        "IMPORTANTE: Siempre incluí AMBOS pasos, nunca solo uno."
    ),
    analyst_injection=(
        "FORMATO REQUERIDO — PERFIL DE LEGISLADOR:\n"
        "Respondé con este formato de ficha:\n\n"
        "**[Nombre del legislador]**\n\n"
        "**Datos personales**: cargo, CUIT, estado civil (si disponible).\n\n"
        "**Patrimonio declarado** (ejercicio 2024):\n"
        "- Bienes al cierre: $X\n"
        "- Deudas al cierre: $Y\n"
        "- Patrimonio neto: $Z\n"
        "- Ingresos del trabajo: $W\n\n"
        "**Desglose de bienes** (por categoría):\n"
        "- Inmuebles: $X (N propiedades)\n"
        "- Automotores: $Y (N vehículos)\n"
        "- Depósitos: $Z\n"
        "- (otras categorías con monto)\n\n"
        "**Personal a cargo**: N empleados. Listar los principales con área.\n\n"
        "Formateá montos en pesos argentinos legibles (ej: $184.120.033)."
    ),
)

_SKILL_PRECIO = SkillDef(
    name="precio",
    description="Cotización rápida de indicadores financieros",
    patterns=(
        re.compile(r"(?:a\s+)?cu[aá]nto\s+(?:est[aá]|sale|cotiza)", _I),
        re.compile(
            r"(?:precio|valor|cotizaci[oó]n)\s+(?:de(?:l)?\s+)?"
            r"(?:d[oó]lar|dolar|usd|euro|real)",
            _I,
        ),
        re.compile(
            r"(?:d[oó]lar|dolar)\s+(?:hoy|actual|blue|oficial|tarjeta|cripto|bolsa|mep|ccl)",
            _I,
        ),
        re.compile(r"riesgo\s+pa[ií]s|embi", _I),
        re.compile(r"canasta\s+b[aá]sica|l[ií]nea\s+de\s+(?:pobreza|indigencia)", _I),
    ),
    planner_injection=(
        "SKILL ACTIVA: COTIZACIÓN RÁPIDA\n"
        "El usuario quiere un dato puntual de precio/cotización.\n"
        "Tu plan debe ser MÍNIMO — un solo paso si es posible:\n"
        "- Para dólar → query_argentina_datos con action 'fetch_dolar'\n"
        "- Para tipo de cambio oficial → query_bcra\n"
        "- Para riesgo país → query_argentina_datos con action 'fetch_riesgo_pais'\n"
        "- Para canasta básica/línea de pobreza → query_series con serie "
        "150.1_LA_POBREZA_0_D_13\n"
        "- Para inflación → query_series con serie 148.3_INIVELNAL_DICI_M_26 "
        "con representation 'percent_change'\n"
        "IMPORTANTE: NO generes pasos innecesarios. Máximo 2 pasos."
    ),
    analyst_injection=(
        "FORMATO REQUERIDO — COTIZACIÓN RÁPIDA:\n"
        "Respondé de forma ULTRA BREVE (máximo 3 oraciones):\n\n"
        "1. El valor actual del indicador en negrita.\n"
        "2. La variación reciente (diaria, semanal o mensual según corresponda).\n"
        "3. Un mini gráfico de línea si hay datos históricos: "
        "<!--CHART:{\"type\":\"line_chart\",...}-->.\n\n"
        "NO des contexto extenso. El usuario quiere el número, no una explicación."
    ),
)

_BUILTIN_SKILLS: tuple[SkillDef, ...] = (
    _SKILL_VERIFICAR,
    _SKILL_COMPARAR,
    _SKILL_PRESUPUESTO,
    _SKILL_PERFIL,
    _SKILL_PRECIO,
)
