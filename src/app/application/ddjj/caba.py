"""Declaraciones juradas de la Ciudad de Buenos Aires: cómo leer sus CSV.

La Secretaría Legal y Técnica publica en data.buenosaires.gob.ar (dataset
`declaraciones-juradas`, CC-BY 2.5 AR) un CSV por año de presentación, de 2015 a
2026 (sin 2020), con las declaraciones de los funcionarios de las jurisdicciones
con rango ministerial.

Lo que se midió el 08-oct-2026:

- **2023 a 2026 vienen en formato ancho**: una fila por declaración (`id_ddjj`),
  con nombre, apellido, cargo, fecha de presentación y el total de cada tipo de
  bien. 10.138 declaraciones en total; en 2026 hay 3.005 de 2.206 personas
  (varias por persona y cargo).
  - No traen deudas: `TOTAL_DEUDAS` está vacía en 2023-2024 y desaparece en
    2025. Por eso estas declaraciones no tienen patrimonio neto.
  - Tampoco traen CUIT, organismo ni tipo de declaración, aunque los metadatos
    del portal dicen CUIL.
  - El encabezado cambia de "AÑO_PRESENTACIÓN" en mayúsculas a
    "anio_presentacion"; se normaliza.
- **2015 a 2022 vienen en formato largo** (campo/valor), distinto cada año: con
  `;` o con `,`, con basura XML ("<ns1"), algunos sin nombre en cada fila. Se
  saltean por ahora; pivotearlos queda para una segunda etapa.
"""

from __future__ import annotations

import csv
import io
import json
import re
import unicodedata
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from app.application.ddjj.oficina_anticorrupcion import parse_monto, poder_de

FUENTE = "caba"
JURISDICCION = "caba"
PAQUETE = "declaraciones-juradas"
URL_DATASET = "https://data.buenosaires.gob.ar/dataset/declaraciones-juradas"
# Los CSV se bajan directo del CDN: la API de CKAN de la Ciudad está detrás de un
# WAF que, desde los servidores de OpenArg, contesta "Request Rejected" (HTML con
# status 200) después del primer pedido (08-oct-2026). El CDN responde con el CSV,
# su `Last-Modified` y un 404 para el año que no existe (2020 no se publicó).
URL_CSV = (
    "https://cdn.buenosaires.gob.ar/datosabiertos/datasets/secretaria-legal-y-tecnica/"
    "declaraciones-juradas/declaraciones-juradas-{anio}.csv"
)
PRIMER_ANIO = 2015


def anios_a_probar(anio_actual: int) -> range:
    """De 2015 al año que viene: el del año próximo aparece apenas empieza."""
    return range(PRIMER_ANIO, anio_actual + 2)


# Columna del CSV → clave en `bienes_por_tipo`.
COMPONENTES: dict[str, str] = {
    "total_bienes_inmuebles": "inmuebles",
    "total_bienes_muebles": "muebles",
    "total_acciones": "acciones",
    "total_fondos": "fondos",
    "total_bonos": "bonos",
    "total_titulos": "titulos",
    "total_dinero_efectivo": "dinero_efectivo",
    "total_dinero_electronico": "dinero_electronico",
}

COLUMNAS_DECLARACION: tuple[str, ...] = (
    "fuente",
    "dj_id",
    "jurisdiccion",
    "poder",
    "nombre",
    "anio",
    "cargo",
    "fecha_presentacion",
    "bienes_cierre",
    "bienes",
    "bienes_por_tipo",
    "corte",
    "archivo_fuente",
    "url_fuente",
)

_RE_ANIO_URL = re.compile(r"declaraciones-juradas-(\d{4})\.csv", re.IGNORECASE)
_RE_FECHA = re.compile(r"(\d{4})-(\d{2})-(\d{2})")


def normalizar_columna(nombre: str) -> str:
    """'AÑO_PRESENTACIÓN' → 'anio_presentacion'."""
    s = (nombre or "").strip().lower().replace("ñ", "ni")
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    return s


def anio_de_url(url: str) -> int | None:
    m = _RE_ANIO_URL.search(url or "")
    return int(m.group(1)) if m else None


@dataclass(frozen=True)
class ArchivoCaba:
    ruta: str
    url: str
    corte: date | None  # fecha de modificación del recurso, si CKAN la da


def leer(ruta: str) -> tuple[list[str], Iterator[dict[str, str]]]:
    """Columnas normalizadas y filas (con las claves normalizadas)."""
    crudo = open(ruta, "rb").read()  # noqa: SIM115 — son CSV de pocos MB
    try:
        texto = crudo.decode("utf-8-sig")
    except UnicodeDecodeError:
        texto = crudo.decode("latin-1")
    lector = csv.reader(io.StringIO(texto, newline=""))
    columnas = [normalizar_columna(c) for c in next(lector, [])]

    def filas() -> Iterator[dict[str, str]]:
        for fila in lector:
            yield dict(zip(columnas, fila, strict=False))

    return columnas, filas()


def es_formato_ancho(columnas: list[str]) -> bool:
    return {"id_ddjj", "anio_presentacion", "total_bienes_inmuebles"} <= set(columnas)


def _fecha(texto: str | None) -> date | None:
    m = _RE_FECHA.search(texto or "")
    if not m:
        return None
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError:
        return None


def _numero_json(valor: Decimal) -> int | float:
    return int(valor) if valor == valor.to_integral_value() else float(valor)


def fila_declaracion(fila: Mapping[str, str | None], archivo: ArchivoCaba) -> tuple | None:
    """Una fila de `cache_ddjj_declaraciones`, en el orden de `COLUMNAS_DECLARACION`."""
    dj_id = (fila.get("id_ddjj") or "").strip()
    anio = (fila.get("anio_presentacion") or "").strip()
    if not dj_id.isdigit() or not anio.isdigit():
        return None
    nombre = " ".join(f"{fila.get('apellido') or ''} {fila.get('nombre') or ''}".upper().split())
    cargo = " ".join((fila.get("cargo") or "").split()) or None
    por_tipo: dict[str, int | float] = {}
    total = Decimal(0)
    for columna, clave in COMPONENTES.items():
        valor = parse_monto(fila.get(columna))
        if valor is None:
            continue
        por_tipo[clave] = _numero_json(valor)
        total += valor
    poder = poder_de(None, cargo)
    return (
        FUENTE,
        int(dj_id),
        JURISDICCION,
        # El dataset es del Poder Ejecutivo porteño: lo que el cargo no ubica en
        # otro poder ("Controlador/A De Faltas", "Miembro de Junta Comunal") es suyo.
        "ejecutivo" if poder == "sin_dato" else poder,
        nombre or None,
        int(anio),
        cargo,
        _fecha(fila.get("fecha_presentacion")),
        total if por_tipo else None,
        total if por_tipo else None,
        json.dumps(por_tipo) if por_tipo else None,
        archivo.corte,
        archivo.url.rsplit("/", 1)[-1],
        archivo.url,
    )
