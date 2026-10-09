"""DDJJ de actividades anteriores y posteriores a la función pública (Oficina Anticorrupción).

Dataset `declaraciones-juradas-de-actividades-anteriores-y-posteriores-a-la-funcion-publica`
de datos.jus.gob.ar (CC-BY 4.0), del Sistema de Monitoreo de Actividades
Privadas y Públicas Anteriores y Posteriores (MAPPAP). Lo presentan las
autoridades (ministros, secretarios, subsecretarios, directores…) al asumir
(actividades anteriores) y al irse (posteriores).

Lo que se midió el 08-oct-2026:

- **Hay un corte por trimestre** (20260331, 20260630, 20260930) y cada uno es
  acumulado desde que el sistema empezó en 2022: de las 1.087 declaraciones del
  corte de diciembre de 2022, 1.073 siguen en el de septiembre de 2026. Alcanza
  con los dos CSV sueltos del último corte.
- **Corte 20260930:** anteriores, 7.078 filas (3.565 declaraciones de 2.007
  personas); posteriores, 1.333 filas (1.091 declaraciones de 797 personas).
- **Encabezados:** el del documento cambia de nombre entre los dos archivos
  (`documento_FODYW_…` y `documento_FOJWP_…`), y "Sin información" es el vacío.
- **Una fila trae varias actividades a la vez:** un empleo en relación de
  dependencia, un trabajo independiente, un cargo público anterior y una
  actividad ad honorem, cada una con su entidad, CUIT, sector y fechas. Acá se
  separan en una fila por actividad.
- **Las actualizaciones repiten actividades ya declaradas:** se queda la del
  documento más nuevo.
"""

from __future__ import annotations

import csv
import io
import re
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import date

FUENTE = "oficina_anticorrupcion"
PAQUETE = "declaraciones-juradas-de-actividades-anteriores-y-posteriores-a-la-funcion-publica"
URL_DATASET = f"https://datos.jus.gob.ar/dataset/{PAQUETE}"

ANTERIOR = "anterior"
POSTERIOR = "posterior"

_VACIO = {"", "sin informacion", "sin información", "-", "no"}
_RE_DOCUMENTO = re.compile(r"^documento_[a-z0-9]+_(nro_documento|fecha_hora_creacion)$", re.I)

COLUMNAS: tuple[str, ...] = (
    "momento",
    "documento",
    "fecha_documento",
    "tramite",
    "cuit",
    "nombre",
    "cargo",
    "organismo",
    "cargo_desde",
    "cargo_hasta",
    "profesion",
    "tipo",
    "entidad",
    "entidad_cuit",
    "sector",
    "puesto",
    "desde",
    "hasta",
    "continua",
    "url_fuente",
)


def momento_de(nombre_recurso: str) -> str | None:
    n = (nombre_recurso or "").lower()
    if "anteriores" in n and "posteriores" not in n:
        return ANTERIOR
    if "posteriores" in n and "anteriores" not in n:
        return POSTERIOR
    return None


def normalizar_encabezado(columna: str) -> str:
    c = (columna or "").lstrip("\ufeff").strip().lower()
    m = _RE_DOCUMENTO.match(c)
    return f"documento_{m.group(1)}" if m else c


def leer(texto: str) -> Iterator[dict[str, str]]:
    lector = csv.reader(io.StringIO(texto, newline=""))
    encabezado = [normalizar_encabezado(c) for c in next(lector, [])]
    for fila in lector:
        yield dict(zip(encabezado, fila, strict=False))


def _texto(fila: Mapping[str, str | None], clave: str) -> str | None:
    valor = " ".join((fila.get(clave) or "").split())
    return None if valor.lower() in _VACIO else valor


def _fecha(texto: str | None) -> date | None:
    s = (texto or "").strip()[:10]
    try:
        return date.fromisoformat(s) if len(s) == 10 else None
    except ValueError:
        return None


def _si(texto: str | None) -> bool | None:
    s = (texto or "").strip().lower()
    return True if s == "si" or s == "sí" else False if s == "no" else None


def _cuit(texto: str | None) -> str | None:
    digitos = re.sub(r"\D", "", texto or "")
    return digitos if len(digitos) == 11 else None


# (tipo, campo de entidad, de cuit, de sector, de puesto, de desde, de hasta, de continuidad)
_ACTIVIDADES: tuple[
    tuple[str, str, str | None, str, str | None, str, str | None, str | None], ...
] = (
    (
        "relacion_dependencia",
        "relacion_dependencia_empleador",
        "relacion_dependencia_empleador_cuit",
        "relacion_dependencia_sector",
        "ultimo_puesto_ocupado_descripcion",
        "relacion_dependencia_fecha_inicio",
        "relacion_dependencia_fecha_cese",
        "relacion_dependencia_continuidad",
    ),
    (
        "trabajo_independiente",
        "trabajo_independiente_razon_social",
        None,
        "trabajo_independiente_sector",
        "categoria_ocupacional_trabajo_independiente",
        "trabajo_independiente_fecha_inicio",
        "trabajo_independiente_fecha_cese",
        "trabajo_independiente_continuidad",
    ),
    (
        "funcion_publica",
        "funcion_publica_organismo",
        None,
        "funcion_publica_sector",
        "funcion_publica_categoria_ocupacional",
        "funcion_publica_fecha_inicio",
        "funcion_publica_fecha_cese",
        "funcion_publica_continuidad",
    ),
    (
        "ad_honorem",
        "actividad_ad_honorem_entidad",
        "actividad_ad_honorem_entidad_cuit",
        "actividad_ad_honorem_sector",
        "actividad_ad_honorem_descripcion",
        "actividad_ad_honorem_fecha_inicio",
        "actividad_ad_honorem_fecha_cese",
        "actividad_ad_honorem_continuidad",
    ),
)


@dataclass(frozen=True)
class Archivo:
    momento: str
    texto: str
    url: str


def filas_actividades(archivo: Archivo) -> Iterator[tuple]:
    """Una tupla por actividad declarada, en el orden de `COLUMNAS`."""
    posterior = archivo.momento == POSTERIOR
    for fila in leer(archivo.texto):
        cuit = _cuit(fila.get("cuit_cuil"))
        documento = _texto(fila, "documento_nro_documento")
        if not cuit or not documento:
            continue
        nombre = " ".join((fila.get("apellidos_nombres") or "").upper().split()) or None
        cargo = _texto(fila, "cargo_cese" if posterior else "cargo_actual")
        descripcion = _texto(fila, "cargo_descripcion")
        if descripcion and (not cargo or cargo.lower() == "otro"):
            cargo = descripcion
        tramite = (fila.get("tramite_tipo") or "").lower()
        base = (
            archivo.momento,
            documento,
            _fecha(fila.get("documento_fecha_hora_creacion")),
            "egreso"
            if posterior
            else ("actualizacion" if "actualizaci" in tramite else "inscripcion"),
            cuit,
            nombre,
            cargo,
            _texto(fila, "cargo_jurisdiccion"),
            None if posterior else _fecha(fila.get("cargo_fecha_inicio")),
            _fecha(fila.get("cargo_fecha_cese")) if posterior else None,
            _texto(fila, "profesion_ocupacion_descripcion") or _texto(fila, "profesion_ocupacion"),
        )
        for tipo, c_entidad, c_cuit, c_sector, c_puesto, c_desde, c_hasta, c_sigue in _ACTIVIDADES:
            entidad = _texto(fila, c_entidad)
            if entidad is None:
                continue
            sector = _texto(fila, c_sector)
            if sector and sector.lower().startswith("otro sector"):
                sector = _texto(fila, f"{c_sector}_otro") or sector
            puesto = _texto(fila, c_puesto) if c_puesto else None
            if tipo == "relacion_dependencia" and not puesto:
                puesto = _texto(fila, "ultimo_puesto_ocupado") or _texto(
                    fila, "puesto_actual_descripcion"
                )
            yield (
                *base,
                tipo,
                entidad,
                _cuit(fila.get(c_cuit)) if c_cuit else None,
                sector,
                puesto,
                _fecha(fila.get(c_desde)),
                _fecha(fila.get(c_hasta)) if c_hasta else None,
                _si(fila.get(c_sigue)) if c_sigue else None,
                archivo.url,
            )
