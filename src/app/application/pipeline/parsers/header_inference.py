"""Inferencia del encabezado de una tabla recién leída: una vez por archivo.

Vivía dentro del sanitizador de `collector_tasks._to_sql_safe`, que corre en
CADA escritura y en CADA chunk. Cada llamada volvía a mirar las primeras filas
del frame que le tocaba y, si alguna "parecía" encabezado, la promovía. Medido
el 04-oct-2026 en prod:

- Encabezados buenos reemplazados por filas de datos. `leyes_sancionadas3.2.csv`
  trae `PROYECTO_ID,CAMARA_SANCIONADORA,...`; la tabla quedó con columnas
  `'HCDN276796 / HCDN276849'`, `'Senado'`, `'2026-08-27T00:00:00'`. La regla de
  "mejor candidato" aceptaba un empate contra un encabezado perfecto, y
  `'2026-08-27T00:00:00'` contaba como texto de encabezado.
- Tablas truncadas al último chunk. Cada chunk de un CSV promovía filas
  distintas, el append fallaba por columnas distintas y el colector hacía DROP +
  recreate: `proyectos_parlamentarios` quedó con 11.089 filas de 111.091.
- Columnas de linaje renombradas a su valor. `_source_dataset_id` se agrega
  antes de escribir y entraba a la inferencia: la columna pasó a llamarse como
  el UUID del dataset, y al perder el `_` dejó de filtrarse como interna.

Por eso, acá:

1. El encabezado se decide UNA vez por archivo, al parsear (`decide_header`),
   y los chunks siguientes reciben las mismas columnas (`apply_header_decision`).
2. Un encabezado `good` no se toca, y un candidato tiene que ser estrictamente
   mejor, nunca un empate.
3. Las columnas con prefijo `_` no participan ni se renombran.

Módulo listado en `parser_fingerprint._PARSER_MODULES`: un cambio acá mueve la
versión del parser que queda registrada en `raw_table_versions`.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass

import pandas as pd

from app.application.pipeline.parsers.header_tokens import (
    KIND_DATA,
    KIND_NUMBER,
    classify_column_name,
)

logger = logging.getLogger(__name__)

LAYOUT_SIMPLE = "simple_tabular"
LAYOUT_PRESENTATION = "presentation_sheet"
LAYOUT_MULTILINE = "header_multiline"
LAYOUT_SPARSE = "header_sparse"
LAYOUT_WIDE = "wide_csv"

HEADER_GOOD = "good"
HEADER_DEGRADED = "degraded"
HEADER_INVALID = "invalid"

PG_NAME_LIMIT = 63


@dataclass(frozen=True)
class LayoutCandidate:
    profile: str
    columns: list[str]
    rows_consumed: int


@dataclass(frozen=True)
class HeaderDecision:
    """Lo que se decidió sobre el primer chunk de un archivo.

    `source_columns` son las columnas tal como las leyó pandas (ya únicas);
    `columns`, las que se escriben. Los chunks siguientes del mismo archivo
    tienen que traer exactamente `source_columns`, y reciben `columns` sin
    consumir ninguna fila: las filas de encabezado sólo existen al principio.
    """

    source_columns: tuple[str, ...]
    columns: tuple[str, ...]
    rows_consumed: int
    layout_profile: str
    header_quality: str


class HeaderMismatchError(ValueError):
    """Un chunk trae columnas distintas de las del primer chunk del archivo."""


def is_internal_column(name: object) -> bool:
    """Columnas de bookkeeping del colector (`_source_dataset_id`, `_source_url`…)."""
    return str(name or "").startswith("_")


def truncate_utf8_bytes(s: str, byte_limit: int) -> str:
    """Truncate string to fit within byte_limit when UTF-8 encoded.

    Postgres truncates identifiers at 63 bytes (NAMEDATALEN-1), not chars.
    Multi-byte chars (e.g. acentos: 'á' = 2 bytes) caused two distinct
    Python strings to collide in byte-space, raising DuplicateColumn.
    """
    encoded = s.encode("utf-8")
    if len(encoded) <= byte_limit:
        return s
    return encoded[:byte_limit].decode("utf-8", errors="ignore")


def make_unique_columns(columns: Iterable[object]) -> list[str]:
    """Normalize column names and guarantee uniqueness after cleanup.

    Names are truncated to Postgres' 63-byte identifier limit *before*
    dedup, using UTF-8 byte length (not char length) to match what
    Postgres actually does. Without this, two long names that share their
    first 63 chars look distinct in Python but collide once Postgres
    truncates them at CREATE TABLE, raising DuplicateColumn. When
    truncation creates a collision, the candidate is suffixed `_N` while
    reserving space so the final name still fits in 63 bytes.
    """
    used: set[str] = set()
    counters: dict[str, int] = {}
    normalized: list[str] = []

    for idx, raw in enumerate(columns):
        if pd.isna(raw):
            base = f"col_{idx}"
        else:
            base = str(raw).replace("\xa0", "").strip()
            if not base or base.lower() == "nan":
                base = f"col_{idx}"

        if len(base.encode("utf-8")) > PG_NAME_LIMIT:
            base = truncate_utf8_bytes(base, PG_NAME_LIMIT)

        counters[base] = counters.get(base, 0) + 1
        if counters[base] == 1:
            candidate = base
        else:
            suffix = f"_{counters[base]}"
            trunc = truncate_utf8_bytes(base, max(1, PG_NAME_LIMIT - len(suffix)))
            candidate = f"{trunc}{suffix}"
        while candidate in used:
            counters[base] += 1
            suffix = f"_{counters[base]}"
            trunc = truncate_utf8_bytes(base, max(1, PG_NAME_LIMIT - len(suffix)))
            candidate = f"{trunc}{suffix}"

        used.add(candidate)
        normalized.append(candidate)

    return normalized


def is_placeholder_column_name(value: object) -> bool:
    text_value = str(value or "").strip()
    if not text_value:
        return True
    lowered = text_value.lower()
    if lowered.startswith("unnamed:"):
        return True
    if re.fullmatch(r"col_[0-9]+", lowered):
        return True
    if re.fullmatch(r"[0-9]+", lowered):
        return True
    return False


def header_quality(columns) -> tuple[int, int, int]:
    normalized = [str(col or "").strip() for col in columns]
    placeholder_count = sum(1 for col in normalized if is_placeholder_column_name(col))
    alpha_count = sum(1 for col in normalized if re.search(r"[A-Za-zÁÉÍÓÚáéíóúÑñ]", col or ""))
    nonempty_count = sum(1 for col in normalized if col)
    return placeholder_count, alpha_count, nonempty_count


def header_numeric_count(columns) -> int:
    return sum(
        1
        for col in (str(col or "").strip() for col in columns)
        if re.fullmatch(r"[0-9]+", col or "")
    )


def header_quality_label(columns) -> str:
    total_columns = max(len(columns), 1)
    placeholder_count, alpha_count, nonempty_count = header_quality(columns)
    numeric_count = header_numeric_count(columns)
    placeholder_ratio = placeholder_count / total_columns
    numeric_ratio = numeric_count / total_columns
    min_nonempty = 1 if total_columns <= 2 else max(3, int(total_columns * 0.4))

    if placeholder_ratio >= 0.35 or nonempty_count < min_nonempty:
        return HEADER_INVALID
    if (
        placeholder_count > 0
        or numeric_ratio >= 0.25
        or alpha_count < max(2, int(total_columns * 0.2))
    ):
        return HEADER_DEGRADED
    return HEADER_GOOD


def normalize_header_token(value: object) -> str:
    if pd.isna(value):
        return ""
    text_value = str(value).replace("\xa0", " ").strip()
    if not text_value or text_value.lower() == "nan":
        return ""
    return re.sub(r"\s+", " ", text_value)


def forward_fill_header_tokens(values) -> list[str]:
    filled: list[str] = []
    last_seen = ""
    for value in values:
        token = normalize_header_token(value)
        if token and not is_placeholder_column_name(token):
            last_seen = token
            filled.append(token)
        else:
            filled.append(last_seen if last_seen else token)
    return filled


def token_looks_like_data(token: str) -> bool:
    normalized = token.strip()
    if not normalized:
        return False
    if re.fullmatch(r"[-+]?[\d.,/%]+", normalized):
        return True
    if re.fullmatch(r"\d{4}[-/]\d{1,2}[-/]\d{1,2}", normalized):
        return True
    # Lo que la versión anterior contaba como texto de encabezado: una fecha
    # ISO con hora (`2026-08-27T00:00:00`, la `T` es una letra), un UUID, una
    # URL, un código de registro (`HCDN285290`) o un expediente
    # (`0011-PE-2024`). Son valores de una fila, nunca nombres de columna.
    return classify_column_name(normalized) in (KIND_DATA, KIND_NUMBER)


def row_looks_like_header(row: list[object]) -> bool:
    tokens = [normalize_header_token(value) for value in row]
    nonempty = [token for token in tokens if token]
    if not nonempty:
        return False

    placeholder_count = sum(1 for token in nonempty if is_placeholder_column_name(token))
    alpha_count = sum(1 for token in nonempty if re.search(r"[A-Za-zÁ-ÿ]", token))
    dataish_count = sum(1 for token in nonempty if token_looks_like_data(token))

    if placeholder_count >= max(1, int(len(nonempty) * 0.6)):
        return False
    if alpha_count < max(1, int(len(nonempty) * 0.4)):
        return False
    if dataish_count > max(1, int(len(nonempty) * 0.5)):
        return False
    return True


def row_has_hierarchy_signal(row: list[object]) -> bool:
    tokens = [
        token
        for token in (normalize_header_token(value) for value in row)
        if token and not is_placeholder_column_name(token)
    ]
    if not tokens:
        return False
    if len(tokens) != len(row):
        return True
    return len(set(tokens)) < len(tokens)


def combine_header_rows(rows: list[list[object]], *, sparse: bool = False) -> list[str]:
    prepared_rows: list[list[str]] = []
    for row in rows:
        tokens = [normalize_header_token(value) for value in row]
        if sparse:
            tokens = forward_fill_header_tokens(tokens)
        prepared_rows.append(tokens)

    combined: list[str | None] = []
    for col_values in zip(*prepared_rows, strict=False):
        parts: list[str] = []
        for token in col_values:
            if not token or is_placeholder_column_name(token):
                continue
            if parts and token == parts[-1]:
                continue
            parts.append(token)
        combined.append(" / ".join(parts) if parts else None)
    return make_unique_columns(combined)


def candidate_is_meaningfully_better(
    current_columns: list[str],
    candidate_columns: list[str],
    *,
    rows_consumed: int,
) -> bool:
    """¿El candidato mejora ESTRICTAMENTE el encabezado actual?

    La primera regla decía `candidate_placeholder <= max(0, current - 2)`: con
    cero placeholders de cada lado el `max` la volvía `0 <= 0` y un empate
    contra un encabezado perfecto contaba como mejora. Ahora tiene que haber al
    menos dos placeholders menos.
    """
    total_columns = max(len(current_columns), 1)
    current_placeholder, current_alpha, current_nonempty = header_quality(current_columns)
    candidate_placeholder, candidate_alpha, candidate_nonempty = header_quality(candidate_columns)

    if candidate_nonempty < max(3, int(total_columns * 0.3)):
        return False

    if candidate_placeholder <= current_placeholder - 2 and candidate_alpha >= max(
        3, current_alpha
    ):
        return True

    if (
        current_placeholder >= max(2, int(total_columns * 0.35))
        and candidate_placeholder <= max(1, int(total_columns * 0.15))
        and candidate_alpha >= max(2, current_alpha - 1)
    ):
        return True

    if (
        rows_consumed >= 2
        and candidate_placeholder < current_placeholder
        and candidate_alpha > current_alpha
    ):
        return True

    return False


def infer_layout_profile(
    df: pd.DataFrame,
    *,
    max_candidate_rows: int = 5,
) -> LayoutCandidate:
    current_columns = make_unique_columns(df.columns)
    best = LayoutCandidate(profile=LAYOUT_SIMPLE, columns=current_columns, rows_consumed=0)

    candidate_rows = min(max_candidate_rows, len(df))
    if df.empty or candidate_rows == 0:
        return best

    # Un encabezado bueno no se reemplaza por nada: el archivo ya dijo cómo se
    # llaman sus columnas. Sin esto, cualquier fila de datos con algo de texto y
    # celdas vacías (la vía `sparse`) le ganaba a `PROYECTO_ID, CAMARA_...`.
    if header_quality_label(current_columns) == HEADER_GOOD:
        return best

    for idx in range(candidate_rows):
        row_values = list(df.iloc[idx])
        if not row_looks_like_header(row_values):
            break
        if idx > 0:
            break
        promoted_columns = make_unique_columns(df.iloc[idx])
        if candidate_is_meaningfully_better(
            current_columns,
            promoted_columns,
            rows_consumed=idx + 1,
        ):
            best = LayoutCandidate(
                profile=LAYOUT_PRESENTATION,
                columns=promoted_columns,
                rows_consumed=idx + 1,
            )

    header_like_prefix = 0
    for idx in range(candidate_rows):
        if row_looks_like_header(list(df.iloc[idx])):
            header_like_prefix += 1
        else:
            break

    for row_count in (2,):
        if candidate_rows < row_count:
            continue
        if header_like_prefix < row_count:
            continue
        header_rows = [list(df.iloc[idx]) for idx in range(row_count)]
        if row_has_hierarchy_signal(header_rows[0]):
            combined_columns = combine_header_rows(header_rows, sparse=False)
            if candidate_is_meaningfully_better(
                current_columns,
                combined_columns,
                rows_consumed=row_count,
            ):
                best = LayoutCandidate(
                    profile=LAYOUT_MULTILINE,
                    columns=combined_columns,
                    rows_consumed=row_count,
                )
        if any(normalize_header_token(value) == "" for row in header_rows for value in row):
            sparse_columns = combine_header_rows(header_rows, sparse=True)
            if candidate_is_meaningfully_better(
                current_columns,
                sparse_columns,
                rows_consumed=row_count,
            ):
                best = LayoutCandidate(
                    profile=LAYOUT_SPARSE,
                    columns=sparse_columns,
                    rows_consumed=row_count,
                )

    return best


def _promotion_candidate(
    df: pd.DataFrame, *, max_candidate_rows: int = 5
) -> tuple[LayoutCandidate, list[str]]:
    """Candidato calculado SÓLO sobre las columnas de datos.

    Devuelve el candidato y la lista completa de nombres que tendría el frame:
    las columnas internas (`_source_dataset_id`, `_source_url`…) conservan su
    nombre y su posición. Antes entraban a la inferencia y salían renombradas
    con su valor — un UUID, una URL —, o rellenadas hacia adelante con la URL de
    al lado cuando venían en None.
    """
    names = [str(c) for c in df.columns]
    data_positions = [i for i, name in enumerate(names) if not is_internal_column(name)]
    simple = LayoutCandidate(profile=LAYOUT_SIMPLE, columns=names, rows_consumed=0)
    if df.empty or not data_positions:
        return simple, names
    data_df = df if len(data_positions) == len(names) else df.iloc[:, data_positions]
    candidate = infer_layout_profile(data_df, max_candidate_rows=max_candidate_rows)
    if candidate.rows_consumed == 0:
        return candidate, names
    new_names = list(names)
    for position, name in zip(data_positions, candidate.columns, strict=True):
        new_names[position] = name
    return candidate, new_names


def maybe_promote_header_row(df: pd.DataFrame, *, max_candidate_rows: int = 5) -> pd.DataFrame:
    if df.empty:
        return df

    candidate, new_names = _promotion_candidate(df, max_candidate_rows=max_candidate_rows)
    if candidate.rows_consumed == 0:
        return df

    current_columns = make_unique_columns(df.columns)
    current_placeholder, current_alpha, _ = header_quality(current_columns)
    best_placeholder, best_alpha, _ = header_quality(candidate.columns)

    promoted = df.iloc[candidate.rows_consumed :].reset_index(drop=True).copy()
    promoted.columns = make_unique_columns(new_names)
    promoted.attrs["layout_profile"] = candidate.profile
    promoted.attrs["header_quality"] = header_quality_label(candidate.columns)
    logger.info(
        "Applied layout_profile=%s consuming %s header rows (placeholders %s -> %s, alpha %s -> %s)",
        candidate.profile,
        candidate.rows_consumed,
        current_placeholder,
        best_placeholder,
        current_alpha,
        best_alpha,
    )
    return promoted


def decide_header(df: pd.DataFrame) -> tuple[pd.DataFrame, HeaderDecision]:
    """Decide el encabezado de un archivo mirando su primer chunk (o el frame entero).

    No modifica `df`. Devuelve el frame con el encabezado aplicado (sin las
    filas de encabezado que haya consumido) y la decisión, para aplicarla tal
    cual a los chunks siguientes con `apply_header_decision`.
    """
    frame = df.copy(deep=False)
    frame.columns = make_unique_columns(frame.columns)
    source_columns = tuple(str(c) for c in frame.columns)

    promoted = maybe_promote_header_row(frame)
    rows_consumed = len(frame) - len(promoted) if promoted is not frame else 0
    columns = tuple(str(c) for c in promoted.columns)
    profile = str(promoted.attrs.get("layout_profile") or LAYOUT_SIMPLE)
    quality = header_quality_label(list(columns))
    promoted.attrs["layout_profile"] = profile
    promoted.attrs["header_quality"] = quality
    decision = HeaderDecision(
        source_columns=source_columns,
        columns=columns,
        rows_consumed=rows_consumed,
        layout_profile=profile,
        header_quality=quality,
    )
    return promoted, decision


def apply_header_decision(df: pd.DataFrame, decision: HeaderDecision) -> pd.DataFrame:
    """Aplica a un chunk posterior el encabezado que decidió el primero.

    Sin volver a inferir nada y sin consumir filas. Si el chunk trae otras
    columnas que el primero, falla en vez de escribir columnas corridas.
    """
    names = tuple(make_unique_columns(df.columns))
    if names != decision.source_columns:
        raise HeaderMismatchError(
            f"chunk columns {list(names)[:5]}... differ from the first chunk "
            f"{list(decision.source_columns)[:5]}..."
        )
    out = df.copy(deep=False)
    out.columns = list(decision.columns)
    out.attrs["layout_profile"] = decision.layout_profile
    out.attrs["header_quality"] = decision.header_quality
    return out
