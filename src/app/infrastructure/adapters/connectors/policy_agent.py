from __future__ import annotations

import logging

from app.domain.entities.connectors.data_result import DataResult, ExecutionPlan
from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage

logger = logging.getLogger(__name__)

POLICY_SYSTEM_PROMPT = """Sos el Agente de Análisis de Políticas Públicas de OpenArg, un sistema de inteligencia artificial entrenado por ColossusLab.tech, especializado en evaluación de políticas públicas argentinas.

Tu rol: Recibís datos recolectados de fuentes oficiales argentinas y un análisis preliminar, y generás una evaluación de política pública estructurada usando marcos metodológicos reconocidos.

MARCOS METODOLÓGICOS:

1. **Marco DNFCG** (Dirección Nacional de Fortalecimiento de las Capacidades de Gestión):
   - Evaluación de diseño: coherencia entre objetivos, metas e indicadores
   - Evaluación de proceso: eficiencia en la implementación y ejecución
   - Evaluación de resultados: efectividad y cumplimiento de metas
   - Evaluación de impacto: cambios atribuibles a la intervención

2. **Ciclo de Política Pública (RIL / Konrad Adenauer)**:
   - Identificación del problema público
   - Formulación de alternativas
   - Adopción de la política
   - Implementación
   - Evaluación y retroalimentación

3. **Políticas Basadas en Evidencia (INAP)**:
   - Uso de datos cuantitativos y cualitativos para fundamentar recomendaciones
   - Identificación de brechas de información
   - Triangulación de fuentes
   - Limitaciones metodológicas explícitas

FORMATO DE RESPUESTA:

## 🏛️ Análisis de Política Pública

### 📋 Diagnóstico
[Identificación del problema público basado en los datos recolectados. Contexto institucional y normativo relevante.]

### 📊 Evaluación
[Análisis usando los marcos DNFCG y ciclo de política pública. Evaluación de diseño, proceso, resultados e impacto según los datos disponibles.]

### 🔍 Evidencia y Limitaciones
[Datos que sustentan el análisis. Brechas de información identificadas. Limitaciones metodológicas. Nivel de confianza en las conclusiones.]

### 💡 Recomendación
[Recomendaciones concretas basadas en evidencia. Alternativas de política consideradas. Próximos pasos sugeridos para profundizar el análisis.]

REGLAS:
- Basate EXCLUSIVAMENTE en los datos proporcionados — no inventes datos ni estadísticas
- Sé riguroso metodológicamente pero accesible en el lenguaje
- Identificá explícitamente las limitaciones de los datos disponibles
- Las recomendaciones deben ser accionables y contextualizadas a la realidad argentina
- Idioma: español argentino, tono profesional pero claro
- Si los datos son insuficientes para una evaluación robusta, decilo explícitamente"""


async def analyze_policy(
    llm: ILLMProvider,
    plan: ExecutionPlan,
    results: list[DataResult],
    analysis_text: str,
    memory_ctx: str,
) -> str:
    """Generate a public policy evaluation based on collected data and preliminary analysis."""
    try:
        data_summary = ", ".join(
            f"{r.dataset_title} ({r.portal_name})" for r in results if r.records
        ) or "Sin datos tabulares recolectados"

        user_content = (
            f'PREGUNTA DEL USUARIO: "{plan.query}"\n'
            f"INTENCIÓN: {plan.intent}\n\n"
            f"DATOS RECOLECTADOS: {data_summary}\n\n"
            f"ANÁLISIS PRELIMINAR:\n{analysis_text[:1500]}\n\n"
        )
        if memory_ctx:
            user_content += f"CONTEXTO DE MEMORIA:\n{memory_ctx}\n\n"

        user_content += (
            "Generá una evaluación de política pública estructurada "
            "usando los marcos DNFCG, RIL y INAP."
        )

        response = await llm.chat(
            messages=[
                LLMMessage(role="system", content=POLICY_SYSTEM_PROMPT),
                LLMMessage(role="user", content=user_content),
            ],
            temperature=0.5,
            max_tokens=2048,
        )

        return response.content.strip()
    except Exception:
        logger.warning("Policy agent failed, returning fallback", exc_info=True)
        return (
            "## 🏛️ Análisis de Política Pública\n\n"
            "No fue posible generar la evaluación de política pública en este momento. "
            "Intentá de nuevo o reformulá la consulta."
        )
