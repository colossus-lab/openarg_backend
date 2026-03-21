"""Request classification: casual, meta, injection, educational, off-topic detection."""

from __future__ import annotations

import random
import re

from app.infrastructure.adapters.search.prompt_injection_detector import is_suspicious
from app.infrastructure.audit.audit_logger import audit_injection_blocked

# ── Patterns ────────────────────────────────────────────────

_GREETING_PATTERN = re.compile(
    r"^("
    r"hola|buenas|buen día|buenos días|buenas tardes|buenas noches"
    r"|hey|qué tal|que tal|qué onda|que onda|cómo estás|como estas"
    r"|cómo andás|como andas|qué hacés|que haces"
    r")[\s!?.,;:]*$",
    re.IGNORECASE,
)

_THANKS_PATTERN = re.compile(
    r"^("
    r"gracias|muchas gracias|genial|perfecto|dale|ok|de una|buenísimo|buenisimo"
    r")[\s!?.,;:]*$",
    re.IGNORECASE,
)

_FAREWELL_PATTERN = re.compile(
    r"^("
    r"chau|adiós|adios|hasta luego|nos vemos|hasta pronto"
    r")[\s!?.,;:]*$",
    re.IGNORECASE,
)

_META_PATTERNS = re.compile(
    r"(qué pod[eé]s hacer|qu[eé] sab[eé]s|cu[aá]les son tus funciones"
    r"|c[oó]mo funcion[aá]s|para qu[eé] serv[ií]s|ayuda"
    r"|qu[eé] sos|qui[eé]n sos|qu[eé] es openarg)",
    re.IGNORECASE,
)

_OFF_TOPIC_PATTERNS = re.compile(
    r"(?:"
    r"escrib[íi](me)?\s+(un\s+)?(poema|cuento|canción|cancion|historia|chiste|ensayo|carta|novela)"
    r"|traduc[íi]|translate|traducime"
    r"|escrib[íi]\s+c[oó]digo|write\s+(me\s+)?(a\s+)?(code|program|script|function)"
    r"|programa\s+en\s+(python|java|javascript|c\+\+|rust|go|php|ruby)"
    r"|receta\s+de\s+cocina|recipe\s+for"
    r"|(?:resolv[eé]|calculá|calculate)\s+(?:\d|la\s+ecuaci[oó]n|equation|integral|derivada)"
    r"|(?:quién|quien)\s+gan[oó]\s+(?:el\s+)?(?:mundial|world\s+cup|champions|superbowl|oscar)"
    r"|hor[oó]scopo|horoscope"
    r"|(?:dibuj[aá]|draw)\s+(?:me\s+)?(?:un|una|a|the)?"
    r"|roleplay|act[uú]a\s+como|pretend\s+you|fingí\s+que"
    r"|(?:cant[aá]|sing)\s+(?:me\s+)?(?:una?\s+)?(?:canci[oó]n|song)"
    r"|(?:cont[aá]|tell)\s+(?:me\s+)?(?:un\s+)?(?:chiste|joke)"
    r"|hac[eé](?:me)?\s+(?:un\s+)?(?:resumen|summary)\s+(?:de|of)\s+(?:un|a)\s+(?:libro|book|película|movie)"
    r")",
    re.IGNORECASE,
)

# ── Responses ───────────────────────────────────────────────

_CASUAL_RESPONSES: dict[str, list[str]] = {
    "greeting": [
        "¡Hola! ¿Qué querés saber sobre datos abiertos de Argentina?",
        "¡Buenas! Estoy listo para ayudarte con datos públicos argentinos.",
        "¡Hola! Preguntame sobre economía, presupuesto, educación o cualquier dato público.",
    ],
    "thanks": [
        "¡De nada! Si tenés más consultas sobre datos públicos, acá estoy.",
        "¡Con gusto! ¿Necesitás analizar algo más?",
        "¡No hay de qué! Preguntame lo que necesites.",
    ],
    "farewell": [
        "¡Hasta luego! Volvé cuando necesites datos.",
        "¡Chau! Que andes bien.",
        "¡Nos vemos! Estoy acá para cuando necesites.",
    ],
    "generic": [
        "¡Hola! ¿Qué querés saber sobre datos abiertos de Argentina?",
        "¿En qué puedo ayudarte hoy?",
    ],
}

_META_RESPONSE = (
    "Soy **OpenArg**, un asistente de inteligencia artificial especializado "
    "en datos abiertos de Argentina.\n\n"
    "Puedo ayudarte con:\n"
    "- **Series de tiempo** — inflación, tipo de cambio, PBI, reservas del BCRA\n"
    "- **Economía** — dólar, riesgo país, cotizaciones\n"
    "- **Datos gubernamentales** — datasets de datos.gob.ar y portales provinciales\n"
    "- **Declaraciones juradas** — patrimonio de diputados nacionales\n"
    "- **Sesiones legislativas** — transcripciones del Congreso\n"
    "- **Georeferenciación** — normalización de direcciones y localidades\n\n"
    "Probá preguntándome algo como: *¿Cómo viene la inflación en los últimos meses?*"
)

_EDUCATIONAL_PATTERNS: dict[re.Pattern[str], str] = {
    re.compile(
        r"qu[eé]\s+(es|son)\s+(los\s+)?datos?\s+abiertos",
        re.IGNORECASE,
    ): (
        "Los **datos abiertos** son datos que cualquier persona puede "
        "acceder, usar y compartir libremente. En Argentina, el portal "
        "datos.gob.ar publica miles de datasets de gobierno nacional, "
        "provincias y municipios.\n\n"
        "Probá preguntarme: *¿Qué datasets hay sobre educación?*"
    ),
    re.compile(r"qu[eé]\s+(es|significa)\s+(el\s+)?pbi", re.IGNORECASE): (
        "El **PBI (Producto Bruto Interno)** es el valor total de todos "
        "los bienes y servicios producidos en un país durante un período "
        "determinado. Es el principal indicador de la actividad "
        "económica.\n\n"
        "Probá preguntarme: *¿Cómo evolucionó el PBI en los últimos años?*"
    ),
    re.compile(r"qu[eé]\s+(es|significa)\s+(la\s+)?inflaci[oó]n", re.IGNORECASE): (
        "La **inflación** es el aumento generalizado y sostenido de "
        "los precios de bienes y servicios en una economía. En "
        "Argentina, el INDEC la mide mensualmente a través del IPC.\n\n"
        "Probá preguntarme: *¿Cuál fue la inflación del último mes?*"
    ),
    re.compile(r"qu[eé]\s+(es|significa)\s+(el\s+)?riesgo\s+pa[ií]s", re.IGNORECASE): (
        "El **riesgo país** mide la diferencia de tasa de interés que "
        "pagan los bonos de un país respecto de los bonos del Tesoro "
        "de EE.UU. Un valor alto indica mayor percepción de riesgo.\n\n"
        "Probá preguntarme: *¿Cuánto está el riesgo país hoy?*"
    ),
    re.compile(r"qu[eé]\s+(es|son)\s+(las\s+)?ddjj", re.IGNORECASE): (
        "Las **DDJJ (Declaraciones Juradas)** son documentos donde "
        "los funcionarios públicos declaran su patrimonio. En OpenArg "
        "tenemos 195 declaraciones juradas de diputados nacionales.\n\n"
        "Probá preguntarme: *¿Quién es el diputado con mayor patrimonio?*"
    ),
    re.compile(r"qu[eé]\s+(es|significa)\s+(el\s+)?tipo\s+de\s+cambio", re.IGNORECASE): (
        "El **tipo de cambio** es el precio de una moneda en términos "
        "de otra. En Argentina se habla del dólar oficial, blue, MEP, "
        "CCL, entre otros.\n\n"
        "Probá preguntarme: *¿A cuánto está el dólar hoy?*"
    ),
    re.compile(r"qu[eé]\s+(es|son)\s+(las\s+)?reservas\s+(del\s+)?bcra", re.IGNORECASE): (
        "Las **reservas del BCRA** son los activos en moneda "
        "extranjera que posee el Banco Central de la República "
        "Argentina. Se usan para respaldar la moneda y las "
        "obligaciones de deuda.\n\n"
        "Probá preguntarme: *¿Cómo vienen las reservas del BCRA?*"
    ),
    re.compile(
        r"qu[eé]\s+(es|significa)\s+(el\s+)?presupuesto\s+(nacional|p[uú]blico)",
        re.IGNORECASE,
    ): (
        "El **presupuesto nacional** es la ley que estima los "
        "ingresos y autoriza los gastos del Estado para cada año "
        "fiscal. Define las prioridades de gasto público.\n\n"
        "Probá preguntarme: *¿Cuánto se destina a educación en el presupuesto?*"
    ),
    re.compile(r"qu[eé]\s+(es|son)\s+(las\s+)?sesiones\s+(del\s+)?congreso", re.IGNORECASE): (
        "Las **sesiones del Congreso** son las reuniones de la "
        "Cámara de Diputados y el Senado donde se debaten y votan "
        "proyectos de ley. Tenemos transcripciones de sesiones "
        "disponibles.\n\n"
        "Probá preguntarme: *¿Qué se debatió sobre educación en el Congreso?*"
    ),
    re.compile(
        r"qu[eé]\s+(es|significa)\s+(el\s+)?[ií]ndice\s+de\s+pobreza",
        re.IGNORECASE,
    ): (
        "El **índice de pobreza** mide el porcentaje de personas u "
        "hogares cuyos ingresos no alcanzan para cubrir una canasta "
        "básica. El INDEC lo publica semestralmente en Argentina.\n\n"
        "Probá preguntarme: *¿Cuál es el porcentaje de pobreza actual?*"
    ),
}

# ── INDEC fallback detection ────────────────────────────────

INDEC_PATTERN = re.compile(
    r"(indec|ipc|inflaci[oó]n|emae|actividad\s+econ[oó]mica"
    r"|pib|producto\s+bruto|comercio\s+exterior|exportaci[oó]n|importaci[oó]n|balanza\s+comercial"
    r"|empleo|desempleo|eph|mercado\s+laboral|trabajo"
    r"|canasta\s+b[aá]sica|cbt|cba"
    r"|salario|sueldo"
    r"|pobreza|indigencia"
    r"|construcci[oó]n|isac|industria|ipi|producci[oó]n\s+industrial"
    r"|supermercado"
    r"|turismo"
    r"|distribuci[oó]n\s+del\s+ingreso|gini|decil"
    r"|balance\s+de\s+pagos|cuenta\s+corriente)",
    re.IGNORECASE,
)

# ── Data step actions ───────────────────────────────────────

DATA_ACTIONS = frozenset(
    (
        "query_sandbox",
        "query_series",
        "query_argentina_datos",
        "query_bcra",
        "query_ddjj",
        "query_sesiones",
        "query_staff",
        "search_ckan",
        "query_georef",
    )
)


# ── Public API ──────────────────────────────────────────────


def get_casual_response(question: str) -> str | None:
    """Return a casual response if the question matches greeting/thanks/farewell."""
    t = question.strip()
    if _GREETING_PATTERN.match(t):
        subtype = "greeting"
    elif _THANKS_PATTERN.match(t):
        subtype = "thanks"
    elif _FAREWELL_PATTERN.match(t):
        subtype = "farewell"
    else:
        return None
    responses = _CASUAL_RESPONSES.get(subtype, _CASUAL_RESPONSES["generic"])
    return random.choice(responses)  # noqa: S311


def get_meta_response(question: str) -> str | None:
    """Return meta/help response if the question asks what OpenArg can do."""
    if _META_PATTERNS.search(question.strip()):
        return _META_RESPONSE
    return None


def get_educational_response(text: str) -> str | None:
    """Return an educational explanation if the question asks about a concept."""
    for pattern, response in _EDUCATIONAL_PATTERNS.items():
        if pattern.search(text.strip()):
            return response
    return None


def is_off_topic(question: str) -> bool:
    """Return True if the question is clearly outside the Argentine open data domain."""
    return bool(_OFF_TOPIC_PATTERNS.search(question))


def classify_request(
    question: str,
    user_id: str,
) -> tuple[str | None, str | None]:
    """Check casual/meta/injection/educational patterns.

    Returns (classification_type, response_text).
    classification_type is one of "casual", "meta", "injection", "educational", "off_topic", or None.
    """
    casual = get_casual_response(question)
    if casual:
        return "casual", casual

    meta = get_meta_response(question)
    if meta:
        return "meta", meta

    suspicious, score = is_suspicious(question)
    if suspicious:
        audit_injection_blocked(user=user_id, question=question, score=score)
        return "injection", (
            "No pude procesar esa consulta. "
            "Probá reformulándola con una pregunta sobre datos públicos argentinos, "
            "por ejemplo: *¿Cuál fue la inflación del último mes?*"
        )

    if is_off_topic(question):
        return "off_topic", (
            "Solo puedo ayudarte con consultas sobre datos abiertos de Argentina, "
            "transparencia gubernamental, presupuesto público y temas afines.\n\n"
            "Probá con algo como:\n"
            "- *¿Cuál fue la inflación del último mes?*\n"
            "- *¿Cuántos empleados tiene el Senado?*\n"
            "- *¿Qué dice la última declaración jurada de [diputado]?*"
        )

    edu = get_educational_response(question)
    if edu:
        return "educational", edu

    return None, None
