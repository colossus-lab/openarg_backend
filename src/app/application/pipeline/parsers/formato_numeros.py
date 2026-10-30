"""¿Con qué separador de miles y de decimales se lee este CSV?

El colector leía todos los CSV con los valores por defecto de pandas: el punto
es decimal y no hay separador de miles. En educación superior
(`00._educ_superior_datos.csv`, hackatón del 30-oct-2026) la columna VALOR
trae «124», «1.475», «2.134»: cantidades de personas con punto de miles. Se
guardó como double, con máximo 999, y los estudiantes de grado de 2023 daban
~5.700 en vez de ~2,2 millones.

Pasarle a pandas ``thousands="."`` no alcanza y es peligroso: borra el punto
en cualquier valor de cualquier columna, sin validar los grupos de tres. Con
esa opción «12.5» se lee 125, la fecha «30.10.2026» se lee 30102026 y la
longitud «-34.6037», -346037. Por eso la decisión es por columna, sobre una
muestra leída como texto, con las clases de ``consultas.numeros``:

- el archivo pasa a formato argentino (``thousands="."``, ``decimal=","``) si
  muestra evidencia: ``MIN_EVIDENCIA`` valores distintos que sólo se leen en
  argentino («1.234.567», «12,5»), o una columna con evidencia de miles (ver
  abajo);
- en formato argentino sólo se leen como número las columnas revisadas: sin
  valores que sólo se leen en inglés, sin texto, y, si tienen ambiguos
  («2.134») y ningún valor argentino propio, sin enteros de 1000 o más (quien
  escribe «2.134» escribe «1.475», no «1475»). Una columna con «1,250» y sin
  valores argentinos propios tampoco: puede ser miles en inglés. Todo lo demás
  va como texto;
- sin evidencia argentina, el archivo se lee como siempre, salvo las columnas
  con «d.ddd» que no tienen evidencia inglesa (``MIN_EVIDENCIA`` valores
  distintos como «12.5»): van como texto. Leídas como decimales podían estar
  mil veces abajo; como texto, ``consultas.numeros`` se niega a calcular en
  vez de adivinar.

**Evidencia de miles en una columna sin valores argentinos.** Enteros cortos
y «58.333» son también decimales ingleses de tres cifras: los minutos por día
de la ENUT (H001 de la revisión del 05-oct, documentado en
``consultas.numeros``). Lo que separa los dos casos son los ceros finales.
Quien escribe los enteros sin decimales («500») escribe «58.33» y no «58.330»;
y quien escribe siempre tres decimales escribe «500.000», no «500». Así que
enteros sueltos más ``MIN_EVIDENCIA`` ambiguos distintos terminados en cero
(«1.470», «2.130») sólo se explican con punto de miles. En el archivo de
educación superior hay 32 entre las primeras mil filas; en la ENUT, ninguno.

Lo que queda: la muestra son las primeras filas. Un valor fuera de la muestra
que no siga el formato de su columna se lee con las opciones del archivo
(«12.5» en una columna de enteros de un archivo argentino daría 125).

Módulo listado en `parser_fingerprint._PARSER_MODULES`.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field

from app.application.consultas.numeros import (
    ESPACIOS,
    FORMATO_AR,
    FORMATO_EN,
    MIN_EVIDENCIA,
    clase_valor,
)

# Cuántas celdas se leen como muestra (filas × columnas), y entre qué topes de
# filas. Un CSV de 2.000 columnas no puede leer 5.000 filas como texto.
CELDAS_MUESTRA = 250_000
MIN_FILAS_MUESTRA = 200
MAX_FILAS_MUESTRA = 5_000


def filas_de_muestra(columnas: int) -> int:
    """Cuántas filas leer de un CSV con ``columnas`` columnas."""
    por_celdas = CELDAS_MUESTRA // max(columnas, 1)
    return max(MIN_FILAS_MUESTRA, min(MAX_FILAS_MUESTRA, por_celdas))


@dataclass
class _Perfil:
    """Lo que trae la muestra de una columna."""

    enteros: set[str] = field(default_factory=set)
    ar: set[str] = field(default_factory=set)
    en: set[str] = field(default_factory=set)
    ambiguos_punto: set[str] = field(default_factory=set)
    ambiguos_coma: set[str] = field(default_factory=set)
    # Valores no vacíos que no son números («s/d», «30.10.2026», un nombre).
    texto: bool = False
    # Algún valor no numérico con dígitos: pandas podría leerlo como número
    # con `thousands="."` («30.10.2026» → 30102026). Sin dígitos queda texto.
    texto_con_digitos: bool = False
    entero_max: int = 0

    @property
    def evidencia_miles(self) -> bool:
        """Enteros sueltos y ambiguos terminados en cero: punto de miles."""
        con_cero = {v for v in self.ambiguos_punto if v.endswith("0")}
        return (
            not (self.texto or self.ar or self.en or self.ambiguos_coma)
            and self.entero_max < 1000
            and len(self.enteros) >= MIN_EVIDENCIA
            and len(con_cero) >= MIN_EVIDENCIA
        )

    @property
    def inglesa(self) -> bool:
        return len(self.en) >= MIN_EVIDENCIA and not self.ar

    @property
    def numerica(self) -> bool:
        """¿Algún valor que pandas podría leer como número?"""
        return self.texto_con_digitos or bool(
            self.enteros or self.ar or self.en or self.ambiguos_punto or self.ambiguos_coma
        )

    @property
    def se_lee_en_argentino(self) -> bool:
        """¿Se puede leer como número con ``thousands="."`` y ``decimal=","``?"""
        if self.texto or self.en:
            return False
        if self.ambiguos_coma and not self.ar:
            return False
        return not (self.ambiguos_punto and not self.ar and self.entero_max >= 1000)


def _perfil(valores: Iterable[object]) -> _Perfil:
    perfil = _Perfil()
    for valor in valores:
        if valor is None or valor != valor:  # None o NaN
            continue
        texto = str(valor).strip(ESPACIOS)
        if not texto:
            continue
        clase = clase_valor(texto)
        if clase is None:
            perfil.texto = True
            perfil.texto_con_digitos |= any(c.isdigit() for c in texto)
        elif clase == "entero":
            perfil.enteros.add(texto)
            perfil.entero_max = max(perfil.entero_max, abs(int(texto)))
        elif clase == FORMATO_AR:
            perfil.ar.add(texto)
        elif clase == FORMATO_EN:
            perfil.en.add(texto)
        elif clase == "ambiguo_punto":
            perfil.ambiguos_punto.add(texto)
        else:
            perfil.ambiguos_coma.add(texto)
    return perfil


@dataclass(frozen=True)
class FormatoCsv:
    """Cómo leer los números de un CSV."""

    # True: ``thousands="."`` y ``decimal=","``. False: lo de pandas.
    argentino: bool = False
    # Posiciones de las columnas que se leen como texto.
    como_texto: tuple[int, ...] = ()

    def opciones_read_csv(self) -> dict:
        """Lo que se agrega a los parámetros de ``pd.read_csv``."""
        opciones: dict = {}
        if self.argentino:
            opciones["thousands"] = "."
            opciones["decimal"] = ","
        if self.como_texto:
            opciones["dtype"] = dict.fromkeys(self.como_texto, str)
        return opciones


def decidir_formato(columnas: Sequence[Iterable[object]]) -> FormatoCsv:
    """El formato de un CSV a partir de una muestra de cada columna, en orden."""
    perfiles = [_perfil(valores) for valores in columnas]
    evidencia_ar = sum(len(p.ar) for p in perfiles if not p.en)
    argentino = evidencia_ar >= MIN_EVIDENCIA or any(p.evidencia_miles for p in perfiles)
    if argentino:
        como_texto = tuple(
            i for i, p in enumerate(perfiles) if p.numerica and not p.se_lee_en_argentino
        )
    else:
        como_texto = tuple(i for i, p in enumerate(perfiles) if p.ambiguos_punto and not p.inglesa)
    return FormatoCsv(argentino=argentino, como_texto=como_texto)
