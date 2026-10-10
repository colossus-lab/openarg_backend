"""Quién es un alto cargo, para los rankings de DDJJ que preguntan por «el gobierno».

El 10-oct-2026, «Top 10 ddjj gobierno» rankeó a los 46.326 declarantes del
Ejecutivo nacional de 2024: un jefe de diseño gráfico de la Fuerza Aérea, un
subcomisario, un inspector de ARCA. Quien pregunta por el gobierno pregunta por
quienes lo conducen. Dante eligió el alcance amplio: el gabinete, los titulares
de organismos y empresas del Estado, los directores nacionales y los embajadores.

El cargo es texto libre y hay que leerlo con el organismo. Lo que se midió en el
CSV de la OA de 2024:

- "Secretario/a de…" aparece 1.349 veces y "Subsecretario/a" 538, pero muchas
  son de universidades ("SECRETARIA DE POSGRADO", organismo "UNIVERSIDAD
  NACIONAL DE CORDOBA"). Cuentan sólo con un organismo de la administración
  central (ministerio, secretaría, jefatura de gabinete, presidencia) o sin
  organismo.
- "Ministro" también es un rango diplomático ("MINISTRO PLENIPOTENCIARIO",
  "MINISTRO EN A EMBAJADA ARGENTINA EN VENEZUELA", "MINISTRO DE SEGUNDA") y hay
  "ASESOR MINISTRO DE ECONOMIA". Sólo cuenta "Ministro de <cartera>".
- "Presidente", "Interventor", "Jefe de Gabinete" aparecen en comisiones
  evaluadoras, registros automotores, fuerzas y municipios ("INTERVENTOR DEL
  REG 01092", "JEFE DE GABINETE | MUNICIPALIDAD DE ROSARIO").
- Milei, Caputo, Werthein y Cuneo Libarona declaran sin organismo: el cargo solo
  tiene que alcanzar (`es_autoridad_ejecutivo`, que usa también `poder_de`).

La Ciudad publica categorías en vez de cargos ("MINISTRO/A", "SECRETARIO/A O
EQUIVALENTE", "DIRECTOR/A GENERAL O EQUIVALENTE"); el director general porteño
es el par del director nacional.
"""

from __future__ import annotations

import re
import unicodedata


def normalizar(texto: str | None) -> str:
    """Mayúsculas, sin tildes, sin el "/A" de género y con un espacio entre palabras."""
    sin_tildes = unicodedata.normalize("NFD", texto or "")
    s = "".join(c for c in sin_tildes if unicodedata.category(c) != "Mn").upper()
    s = re.sub(r"/[AO]\b", "", s)  # "DIRECTOR/A" → "DIRECTOR", "MINISTRO/A" → "MINISTRO"
    s = re.sub(r"[^A-Z0-9]+", " ", s)
    return " ".join(s.split())


def _re(*patrones: str) -> re.Pattern[str]:
    return re.compile("|".join(f"(?:{p})" for p in patrones))


# ── Ejecutivo ────────────────────────────────────────────────────────────────

# Alcanzan con el cargo, haya o no organismo. Son también los que ubican el
# poder cuando el organismo viene vacío.
_CUPULA = _re(
    r"^(VICE ?)?PRESIDENTE DE LA NACION\b",
    r"^(VICE ?)?JEFE DE GABINETE DE MINISTROS\b",
    r"^VICE ?JEFE DE GABINETE\b",
    # "Ministro de Economía", "Ministro de Relac. Ext. Com. Int. y Culto", y sin
    # el "de": "MINISTRA RELACIONES EXTERIORES Y CULTO" (Mondino), "Ministro
    # Desregulación y Transformación del Estado" (Sturzenegger). No "Ministro de
    # Segunda/Primera", "Ministro Plenipotenciario" ni "Ministro en Embajada"
    # (rangos diplomáticos), ni "Ministro de la Corte" (judicial, lo ubica
    # `poder_de`).
    r"^MINISTR[OA] (?!(DE )?(SEGUNDA|PRIMERA|LA CORTE)\b)(?!PLENIPOTENCIARI|EN\b|CONSEJER)\w",
)

# Secretarios y subsecretarios: con organismo de la administración central o sin
# organismo.
_SECRETARIO = _re(r"^SECRETARI[OA]S? (DE|DEL|GENERAL)\b", r"^SUB ?SECRETARI[OA]\b")
_ORGANISMO_CENTRAL = _re(
    r"^MINISTERIO\b",
    r"^SECRETARIA\b",
    r"^SUBSECRETARIA\b",
    r"^JEFATURA DE GABINETE\b",
    r"^VICE ?JEFATURA DE GABINETE\b",
    r"^PRESIDENCIA DE LA NACION\b",
)

# Titulares de organismos y empresas del Estado: con organismo, que no sea una
# universidad ni una fuerza.
_TITULAR = _re(
    r"^PRESIDENT[EA]\b",
    r"^VICE ?PRESIDENT[EA]\b",
    r"^DIRECTOR[A]? EJECUTIV[OA]\b",
    # "ADMINISTRADOR" o "ADMINISTRADOR DE DNV", no "Administrador de Sistema ARCA".
    r"^ADMINISTRADOR[A]?( (GENERAL|NACIONAL|FEDERAL|DE LA|DEL|DE)\b(?! SISTEMA)|$)",
    r"^INTERVENTOR[A]?\b",
)

# Directores nacionales y embajadores: el alcance amplio que eligió Dante.
_DIRECTOR_NACIONAL = re.compile(r"^DIRECTOR[A]? NACIONAL\b")
_EMBAJADOR = re.compile(r"^EMBAJADOR[A]?\b")

# Lo que hace que un cargo con nombre de autoridad no lo sea.
_CARGO_EXCLUIDO = _re(
    # rangos diplomáticos ("Embajador Extraordinario y Plenipotenciario" sí es
    # embajador: `_EMBAJADOR` se mira antes que esto)
    r"^MINISTR[OA] PLENIPOTENCIARI",
    r"\bEMBAJADA\b",
    r"\bCONSEJERO\b",
    r"\bCONSUL\b",
    r"^SECRETARI[OA] DE (PRIMERA|SEGUNDA|TERCERA)\b",
    # universidades
    r"\bPOSGRADO\b",
    r"\bACADEMIC",
    r"\bEXTENSION\b",
    r"\bESTUDIANTIL",
    r"\bFACULTAD",
    r"\bUNIVERSI",
    r"\bDECAN",
    # registros automotores ("INTERVENTOR DEL REG 01092", "RNPA", "RRSS")
    r"\bREG\b",
    r"\bREGISTRO",
    r"\bRNPA\b",
    r"\bRRSS\b",
    r"\bAUTOMOTOR",
    # intervienen o administran una dependencia, no el organismo
    # ("INTERVENTOR DPTO ADM CONTABLE", "ADMINISTRADOR DE ADUANA")
    r"\bDEPTO\b",
    r"\bDPTO\b",
    r"\bDEPARTAMENTO\b",
    r"\bDIVISION\b",
    r"\bSECCION\b",
    r"\bADUANA\b",
    # cuerpos colegiados: presiden una comisión, no un organismo
    r"\bCOMISION\b",
    r"\bCOMITE\b",
    r"\bJURADO\b",
    r"\bJUNTA\b",
    r"\bCRE\b",
    # el Poder Judicial también tiene secretarios
    r"\bJUZG",
    r"\bJUZGADO\b",
    r"\bTRIBUNAL\b",
    r"\bFISCALIA\b",
    r"\bDEFENSORIA\b",
    r"\bCAMARA\b",
    r"\bSALA\b",
)

_ORGANISMO_EXCLUIDO = _re(
    r"\bUNIVERSI",
    r"\bFACULTAD\b",
    r"\bEDITORIAL UNIVERSITARIA\b",
    r"\bFUERZA AEREA\b",
    r"\bEJERCITO\b",
    r"\bARMADA\b",
    r"\bGENDARMERIA\b",
    r"\bPREFECTURA\b",
    r"\bPOLICIA\b",
    r"\bMUNICIPALIDAD\b",
    r"\bREGISTRO",
    r"\bAUTOMOTOR",
)

# ── Otros poderes ────────────────────────────────────────────────────────────

_LEGISLADOR = _re(
    # Los mismos errores de tipeo que `oficina_anticorrupcion._PODER_POR_CARGO_FUERTE`.
    r"^(DIPUTAD|DIUTAD|DIPURAD)[OA]S? (NACIONAL|DE LA NACION)\b",
    r"^SENADOR[A]? (NACIONAL|DE LA NACION)\b",
)
_JUDICIAL = _re(
    r"^JUEZ[A]?\b",
    r"^CAMARISTA\b",
    r"^MINISTR[OA] DE LA CORTE\b",
    r"^PROCURADOR[A]? GENERAL\b",
    r"^DEFENSOR[A]? GENERAL\b",
)

# ── Ciudad de Buenos Aires ───────────────────────────────────────────────────

_CIUDAD = _re(
    r"^(VICE ?)?JEF[EA] DE GOBIERNO\b",
    r"^MINISTR[OA]\b",
    r"^SECRETARI[OA]\b",
    r"^SUB ?SECRETARI[OA]\b",
    r"^DIRECTOR[A]? GENERAL\b",
    r"^DIRECTOR[A]? EJECUTIV[OA]\b",
    r"^PRESIDENT[EA]\b",
    r"^VICE ?PRESIDENT[EA]\b",
    r"^PROCURADOR[A]? GENERAL\b",
    r"^SINDIC[OA] GENERAL\b",
)


def es_autoridad_ejecutivo(cargo: str | None) -> bool:
    """Si el cargo, sin mirar el organismo, es del Ejecutivo nacional: Presidente,
    Jefe de Gabinete, ministros de una cartera, secretarios, subsecretarios,
    directores nacionales y embajadores. Es lo que ubica a Milei o a Caputo en el
    Ejecutivo cuando declaran sin organismo (192 altos cargos de 2024 quedaban en
    `sin_dato`)."""
    car = normalizar(cargo)
    if car.startswith(("CANDIDAT", "ASESOR")):
        return False
    if _EMBAJADOR.search(car) or _DIRECTOR_NACIONAL.search(car):
        return True
    if _CARGO_EXCLUIDO.search(car):
        return False
    return bool(_CUPULA.search(car) or _SECRETARIO.search(car))


def es_alto_cargo(
    cargo: str | None,
    organismo: str | None,
    poder: str | None,
    *,
    sector: str | None = None,
    ciudad: bool = False,
) -> bool:
    """Si la declaración es de un alto cargo (ver el docstring del módulo).

    `sector` es el de la OA. Con "PRIVADO", el organismo es otra actividad de la
    persona ("presidente | FAMAR FUEGUINA SA"), así que un "presidente" no es el
    titular de un organismo del Estado. El cargo de la cúpula manda igual: la baja
    de Mondino trae de organismo "BANCO ROELA SA".
    """
    car = normalizar(cargo)
    if not car or car.startswith(("CANDIDAT", "ASESOR")):
        return False
    if ciudad:
        return bool(_CIUDAD.search(car))
    if poder == "legislativo":
        return bool(_LEGISLADOR.search(car))
    if poder in ("judicial", "ministerio_publico"):
        return bool(_JUDICIAL.search(car))
    if _EMBAJADOR.search(car):
        return True
    if _CARGO_EXCLUIDO.search(car):
        return False
    org = normalizar(organismo)
    if _CUPULA.search(car):
        return True
    if _SECRETARIO.search(car):
        return not org or bool(_ORGANISMO_CENTRAL.search(org))
    if org and _ORGANISMO_EXCLUIDO.search(org):
        return False
    if _TITULAR.search(car):
        return bool(org) and normalizar(sector) != "PRIVADO"
    return bool(_DIRECTOR_NACIONAL.search(car))
