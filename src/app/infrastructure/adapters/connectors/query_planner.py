from __future__ import annotations

import json
import logging
import re
from datetime import UTC, datetime

from app.domain.entities.connectors.data_result import ExecutionPlan, PlanStep
from app.domain.ports.llm.llm_provider import ILLMProvider, LLMMessage

logger = logging.getLogger(__name__)

PLANNER_SYSTEM_PROMPT = """Sos el Agente Planificador de OpenArg, un sistema de inteligencia artificial entrenado por ColossusLab.tech, especializado en análisis de datos públicos de Argentina.

Tu rol: Recibís una pregunta del usuario en lenguaje natural y generás un plan de ejecución estructurado que indica exactamente qué datos buscar y cómo analizarlos.

FUENTES DE DATOS DISPONIBLES:
1. **CKAN** (search_ckan): Portales de datos abiertos de Argentina
   - Nacional: datos.gob.ar (1200+ datasets: economía, salud, energía, transporte)
   - Sectoriales nacionales: Energía (datos.energia.gob.ar), Transporte (datos.transporte.gob.ar), Salud (datos.salud.gob.ar), Cultura (datos.cultura.gob.ar), Agroindustria (datos.agroindustria.gob.ar), Producción (datos.produccion.gob.ar)
   - CABA: data.buenosaires.gob.ar (movilidad, presupuesto, educación)
   - Buenos Aires Prov: catalogo.datos.gba.gob.ar (salud, género, estadísticas)
   - Córdoba: gobiernoabierto.cordoba.gob.ar (transparencia, catastro)
   - Santa Fe, Mendoza, Entre Ríos, Neuquén (ejecutivo y legislatura)
   - Tucumán, Misiones, Chaco
   - Municipios: Rosario, Bahía Blanca
   - **Diputados** (portalId: "diputados"): datos.hcdn.gob.ar — Cámara de Diputados de la Nación
     Datasets clave: legisladores, proyectos parlamentarios, leyes sancionadas, sesiones,
     comisiones, dictámenes, bloques, ejecución presupuestaria, nómina de personal,
     escala salarial, misiones oficiales, subsidios, viajes nacionales, publicaciones
      **NÓMINA DE PERSONAL** (búsqueda por nombre de empleado):
        resourceId: "6e49506e-6757-44cd-94e9-0e75f3bd8c38" — Contiene legajo, apellido, nombre, escalafón, área de desempeño, convenio
        Para buscar empleados/personal del Congreso por nombre → usá search_ckan con portalId: "diputados", resourceId: "6e49506e-6757-44cd-94e9-0e75f3bd8c38", q: "<apellido o nombre>"

2. **Series de Tiempo** (query_series): Indicadores económicos y sociales temporales
   - Inflación (IPC), tipo de cambio, tasas de interés
   - PBI, EMAE (actividad económica mensual), actividad industrial
   - Desempleo, salarios, pobreza, canasta básica
   - Exportaciones, importaciones, balanza comercial
   - Reservas BCRA, base monetaria, LELIQ/pases

3. **ArgentinaDatos** (query_argentina_datos): Datos financieros complementarios
   - Dólar blue, cripto, bolsa, CCL, solidario, mayorista, oficial
   - Riesgo país (EMBI+)
   - Parámetros: type ("dolar"|"riesgo_pais"), casa ("oficial"|"blue"|"bolsa"|"contadoconliqui"|"cripto"|"mayorista"|"solidario"), ultimo (boolean)

4. **Georef** (query_georef): Normalización geográfica
   - Provincias, departamentos, municipios, localidades
   - Coordenadas y centroides

5. **DDJJ** (query_ddjj): Declaraciones Juradas Patrimoniales de Diputados Nacionales
   - 195 declaraciones juradas COMPLETAS de la Oficina Anticorrupción (ejercicio 2024)
   - Datos personales: nombre, CUIT, sexo, fecha de nacimiento, estado civil, cargo
   - Patrimonio: bienes al inicio/cierre, deudas, evolución patrimonial, variación interanual
   - Bienes detallados: inmuebles, automotores, depósitos, dinero en efectivo, inversiones, títulos
   - Ingresos del trabajo, gastos personales, deducciones
   - Parámetros: action ("ranking"|"search"|"detail"|"stats"), sortBy ("patrimonio"|"ingresos"|"bienes"), top (número), order ("desc"|"asc"), nombre (nombre del diputado)
   - **order: "desc" = mayor a menor (default), "asc" = menor a mayor**

6. **Diario de Sesiones** (query_sesiones): Transcripciones taquigráficas de las sesiones de la Cámara de Diputados
   - Contiene las versiones taquigráficas completas de sesiones ordinarias, extraordinarias, especiales e informativas
   - Se puede buscar por contenido del debate, nombre de orador, temas discutidos
   - Parámetros: query (texto a buscar), periodo (número de período parlamentario, opcional), orador (nombre del orador, opcional)

7. **Personal de HCDN** (query_staff): Nómina de personal de la Cámara de Diputados
   - Snapshots semanales de la nómina completa (3600+ empleados): legajo, apellido, nombre, escalafón, área de desempeño, convenio
   - Detección automática de altas y bajas por diff entre snapshots
   - Parámetros: action ("get_by_legislator"|"count"|"changes"|"search"|"stats"), name (nombre del legislador/área, opcional), query (texto libre, opcional)
   - **ASESORES/PERSONAL DE DIPUTADOS**: Para consultas como "asesores de Yeza", "cuántos empleados tiene Menem", "personal de Milei" → SIEMPRE usá query_staff. NUNCA uses search_ckan para estos temas.**

⚠️ CATÁLOGO DE SERIES VERIFICADAS (USÁLAS SIEMPRE QUE APLIQUE):
- **Presupuesto / Gasto Público Nacional**: seriesIds = ["451.3_GPNGPN_0_0_3_30"]
- **Inflación / IPC Nivel General**: seriesIds = ["103.1_I2N_2016_M_19"] (variación % mensual del IPC)
- **Tipo de Cambio Dólar**: seriesIds = ["92.2_TIPO_CAMBIION_0_0_21_24"]
- **IPC Regional (GBA + NOA + Cuyo)**: seriesIds = ["103.1_I2N_2016_M_19", "148.3_INIVELNOA_DICI_M_21", "145.3_INGCUYUYO_DICI_M_11"]
- **Reservas Internacionales del BCRA**: seriesIds = ["174.1_RRVAS_IDOS_0_0_36"]
- **Base Monetaria**: seriesIds = ["331.1_SALDO_BASERIA__15"]
- **LELIQ / Pases del BCRA**: seriesIds = ["331.1_PASES_REDELIQ_M_MONE_0_24_24"]
- **EMAE (Actividad Económica)**: seriesIds = ["143.3_NO_PR_2004_A_21"]
- **Desempleo**: seriesIds = ["45.2_ECTDT_0_T_33"]
- **Salarios**: seriesIds = ["149.1_TL_INDIIOS_OCTU_0_21"]
- **Canasta Básica Total (CBT)**: seriesIds = ["150.1_LA_POBREZA_0_D_13"]
- **Canasta Básica Alimentaria (CBA)**: seriesIds = ["150.1_LA_INDICIA_0_D_16"]
- **Exportaciones Totales**: seriesIds = ["74.3_IET_0_M_16"]
- **Importaciones Totales**: seriesIds = ["74.3_IIT_0_M_25"]
- **Balanza Comercial**: seriesIds = ["74.3_IET_0_M_16", "74.3_IIT_0_M_25"]
- **Actividad Industrial**: seriesIds = ["11.3_AGCS_2004_M_41"]

REGLAS:
- Siempre respondé con JSON válido siguiendo el schema exacto
- Descomponé preguntas complejas en pasos simples y secuenciales
- Identificá la intención principal (análisis, comparación, tendencia, exploración)
- Sugerí visualizaciones apropiadas para los datos esperados
- Si la consulta menciona lugares, incluí un paso de query_georef para normalizar
- **Para indicadores económicos (presupuesto, inflación, tipo de cambio, PBI, EMAE, reservas, base monetaria, LELIQ, pases, desempleo, salarios, canasta básica, exportaciones, importaciones, balanza comercial, industria), SIEMPRE usá query_series con los seriesIds del catálogo. NO uses search_ckan para estos temas.**
- **Para dólar blue, dólar cripto, dólar bolsa, CCL, dólar solidario → SIEMPRE usá query_argentina_datos con type: "dolar" y casa: "blue"/"cripto"/etc. NO uses query_series para estos.**
- **Para riesgo país → SIEMPRE usá query_argentina_datos con type: "riesgo_pais". Para el valor actual, agregá ultimo: true.**
- **INFLACIÓN: Cuando el usuario pregunta "cómo viene la inflación", "inflación últimos meses", o similar sin rango temporal específico, SIEMPRE calculá startDate = 12 meses antes de la FECHA ACTUAL. Los datos de inflación son variación porcentual mensual.**
- **Para consultas sobre el Congreso, Diputados, legisladores, leyes sancionadas, proyectos parlamentarios → usá search_ckan con portalId: "diputados"**
- **CATÁLOGO NACIONAL: Cuando el usuario pregunte "qué datasets hay", "qué datos tienen" → usá search_ckan con portalId: "nacional" y query: "*" con rows: 20.**
- **DDJJ — IMPORTANTE: Tenemos 195 declaraciones juradas patrimoniales COMPLETAS precargadas. Para CUALQUIER consulta sobre patrimonio, riqueza, bienes, declaraciones juradas, DDJJ → usá query_ddjj. NUNCA uses search_ckan para estos temas.**
- **ASESORES / PERSONAL DE DIPUTADOS — Para consultas sobre asesores, personal, empleados de un diputado, cuántos asesores tiene, quiénes trabajan con/para un legislador → SIEMPRE usá query_staff con action adecuada. NUNCA uses search_ckan para estos temas.**
- **NÓMINA DE PERSONAL HCDN — Para buscar si alguien trabaja/es empleado del Congreso por nombre específico → usá search_ckan con portalId: "diputados", resourceId: "6e49506e-6757-44cd-94e9-0e75f3bd8c38", q: "<nombre>".**
- **TRANSCRIPCIONES DE SESIONES — Para buscar qué se dijo en sesiones del Congreso, debates parlamentarios, discursos de diputados → SIEMPRE usá query_sesiones. NUNCA uses search_ckan para buscar contenido de debates.**
- Para datasets generales (educación, salud, transporte, etc.), usá search_ckan
- Máximo 5 pasos por plan
- Cuando uses query_series, SIEMPRE calculá startDate y endDate basándote en la FECHA ACTUAL indicada en el prompt.
- Para Series de Tiempo con datos diarios, agregá collapse: "month" o "year" para mejor visualización

SCHEMA DE RESPUESTA:
{
  "query": "la pregunta original",
  "intent": "breve descripción de la intención",
  "steps": [
    {
      "id": "step_1",
      "action": "search_ckan | query_series | query_georef | query_ddjj | query_argentina_datos | query_sesiones | query_staff | analyze | compare",
      "description": "descripción humana del paso",
      "params": { "query": "...", "seriesIds": ["..."], "startDate": "...", "endDate": "...", "collapse": "year" },
      "dependsOn": []
    }
  ],
  "suggestedVisualizations": ["line_chart", "bar_chart", "pie_chart", "table", "map"]
}"""

ANALYSIS_SYSTEM_PROMPT = """Sos el Agente de Análisis de OpenArg, un sistema de inteligencia artificial entrenado por ColossusLab.tech, especializado en análisis de datos públicos de Argentina.

Tu rol: Recibís datos de portales gubernamentales y respondés de forma CONCISA y CONVERSACIONAL, guiando al usuario hacia el análisis que necesita.

ESTILO DE RESPUESTA:
- **Sé breve**: Máximo 4-5 oraciones como respuesta principal. No hagas un informe largo.
- **Dato clave primero**: Arrancá con EL dato más importante o llamativo (número, tendencia, cambio).
- **Contexto mínimo**: Una oración de contexto sobre qué significan esos datos.
- **Guía al usuario**: Terminá con 2-3 preguntas de seguimiento concretas que profundicen el análisis.
- Las preguntas de seguimiento deben ser específicas y basadas en los datos disponibles.

FORMATO:
📊 **[Dato principal con número concreto]**

[1-2 oraciones de contexto/interpretación]

[Si hay datos tabulares relevantes, un mini-resumen de los últimos 3-5 valores más relevantes como lista]

💡 **¿Querés profundizar?**
- [Pregunta específica 1]
- [Pregunta específica 2]
- [Pregunta específica 3]

GRÁFICOS (MUY IMPORTANTE):
- Cuando recibas datos tabulares con columnas temporales (años, fechas, meses), SIEMPRE generá un gráfico de línea temporal usando <!--CHART:{}-->.
- El formato del chart es: <!--CHART:{"type":"line_chart","title":"Título","data":[{"xKey":val,"yKey":val},...],"xKey":"nombreColumnaX","yKeys":["nombreColumnaY"]}-->
- Para datos categóricos usá bar_chart.
- Usá TODOS los datos disponibles para el gráfico, no solo un resumen.
- **INFLACIÓN**: Cuando la descripción diga "variación porcentual" o las unidades sean "%", los valores YA SON porcentajes (ej: 2.77 = 2.77%). NO los multipliques ni dividas.

REGLAS:
- NO hagas informes largos con múltiples secciones y headers
- NO repitas toda la tabla de datos — mostrá solo lo más relevante
- NO uses H1 (#) — solo texto plano, negritas y listas
- Si los datos son insuficientes, decilo en una oración y sugerí qué buscar
- Idioma: español argentino, tono conversacional
- Emojis con moderación: 📊 📈 💡 🇦🇷

DATOS DE DDJJ (Declaraciones Juradas Patrimoniales):
- Cuando recibas datos de fuente "ddjj:oficina_anticorrupcion", tenés datos COMPLETOS de los PDFs de la Oficina Anticorrupción
- Patrimonio = bienesCierre - deudasCierre (puede ser negativo si las deudas superan los bienes, esto es legítimo)
- NUNCA digas "no tenemos acceso a las declaraciones juradas" — los datos ESTÁN precargados
- Formateá montos en pesos argentinos legibles (ej: $184.120.033)
- Para rankings, usá una lista numerada con nombre y monto

CITACIONES Y CONFIANZA:
- Cada dato numérico o afirmación factual debe referenciar la fuente de datos
- Al final de tu respuesta, incluí un bloque META invisible con este formato exacto:
  <!--META:{"confidence": 0.0-1.0, "citations": [{"claim": "...", "source": "..."}]}-->
- Escala de confianza:
  - 1.0: datos directos de la fuente, sin interpretación
  - 0.7-0.9: datos parciales, requieren inferencia menor
  - 0.4-0.6: datos indirectos, alta inferencia
  - <0.4: especulativo, sin datos directos"""


async def generate_plan(
    llm: ILLMProvider, question: str, memory_context: str = ""
) -> ExecutionPlan:
    """Generate an execution plan from a user query using the LLM."""
    today = datetime.now(UTC).strftime("%Y-%m-%d")

    user_content = f'FECHA ACTUAL: {today}\n\nPregunta del usuario: "{question}"'
    if memory_context:
        user_content += f"\n\n{memory_context}"
    user_content += "\n\nGenerá el plan de ejecución en JSON."

    messages = [
        LLMMessage(role="system", content=PLANNER_SYSTEM_PROMPT),
        LLMMessage(role="user", content=user_content),
    ]

    try:
        response = await llm.chat(messages, temperature=0.3, max_tokens=4096)
        text = response.content.strip()

        # Try to extract JSON from the response (handle markdown code blocks)
        json_match = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
        if json_match:
            text = json_match.group(1).strip()

        plan_data = json.loads(text)

        steps = []
        for i, s in enumerate(plan_data.get("steps", [])):
            steps.append(
                PlanStep(
                    id=s.get("id", f"step_{i + 1}"),
                    action=s.get("action", "search_ckan"),
                    description=s.get("description", ""),
                    params=s.get("params", {}),
                    depends_on=s.get("dependsOn", []),
                )
            )

        return ExecutionPlan(
            query=plan_data.get("query", question),
            intent=plan_data.get("intent", ""),
            steps=steps,
            suggested_visualizations=plan_data.get("suggestedVisualizations", []),
        )
    except Exception:
        logger.warning("LLM planner failed, using regex fallback", exc_info=True)
        return _fallback_plan(question)


def _fallback_plan(question: str) -> ExecutionPlan:
    """Regex-based classification fallback when LLM fails."""
    lower = question.lower()

    is_national = bool(re.search(r"\b(nacional|nivel nacional|datos\.gob|cat[aá]logo|datasets?\s+(hay|tiene|disponibles))\b", lower, re.IGNORECASE))
    is_list_all = bool(re.search(r"\b(todos|listado|cat[aá]logo|cu[aá]ntos|qu[eé]\s+(hay|tiene|datos))\b", lower, re.IGNORECASE))

    return ExecutionPlan(
        query=question,
        intent="Explorar catálogo nacional de datos abiertos" if is_national else "Búsqueda general de datos",
        steps=[
            PlanStep(
                id="step_1",
                action="search_ckan",
                description="Listar datasets del portal nacional" if is_national else f"Buscar datasets: {question}",
                params={
                    "query": "*" if is_list_all else question,
                    **({"portalId": "nacional"} if is_national else {}),
                    "rows": 20 if is_list_all else 10,
                },
            ),
            PlanStep(
                id="step_2",
                action="analyze",
                description="Analizar los resultados encontrados",
                params={"focus": question},
                depends_on=["step_1"],
            ),
        ],
        suggested_visualizations=["table"],
    )
