"""Find our fields in a source that renamed them — and know when it can't.

The HCDN payroll broke on 2026-08-10 because the portal changed `Apellido` to
`APELLIDO` and `Área de Desempeño` to `ESTRUCTURA`. The connector asked for two
exact spellings of each name, matched neither, and mapped 3,743 employees to
blank strings that the upsert collapsed into one row. Nothing failed; the mart
served that row as the payroll of the Chamber of Deputies for three weeks.

`senado_staff_tasks` reads `ID`, `APELLIDO`, `NOMBRE`, `BLOQUE`, `PROVINCIA` the
same literal way and would fail the same silent way.

**What each tier can actually do, because the distinction is the whole design:**

| tier | resuelve | no resuelve |
|---|---|---|
| exacto | la grafía que ya conocíamos | cualquier cambio |
| normalizado | mayúsculas, acentos, espacios, guiones | un renombre real |
| modelo | un renombre, leyendo los **valores** | una fuente que dejó de traer el dato |

Of the six fields the portal changed, **five were only a case change** — the
normalized tier alone recovers those, deterministically and for free. The sixth,
`Área de Desempeño` → `ESTRUCTURA`, is a genuine rename: no amount of string
processing gets there, because the two names share nothing. That one needs to be
read for meaning, which is what the model tier is for and a regex is not.

**And it must be able to refuse.** A field marked `identity=True` — the key the
upsert dedupes on — that cannot be mapped is not a partial success. Writing that
batch destroys a good snapshot, so the caller is told the mapping failed and
writes nothing. That refusal is worth more than the two tiers above it.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text

logger = logging.getLogger(__name__)

_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def normalize_key(name: object) -> str:
    """Case, accents and punctuation removed, so only the wording remains.

    `Área de Desempeño`, `AREA DE DESEMPENO` and `area_de_desempeno` all collapse
    to `areadedesempeno`. `ESTRUCTURA` does not, and that is the honest outcome:
    a rename is not a spelling difference and pretending otherwise would map
    fields by accident.
    """
    text = unicodedata.normalize("NFKD", str(name))
    text = "".join(c for c in text if not unicodedata.combining(c))
    return _NON_ALNUM.sub("", text.lower())


@dataclass(frozen=True)
class FieldSpec:
    """One field we need, and the source spellings we already know."""

    name: str
    aliases: tuple[str, ...] = ()
    # The key the upsert dedupes on. Unmapped, the whole batch collapses — so
    # this is the field whose absence must stop the write rather than degrade it.
    identity: bool = False

    def candidates(self) -> tuple[str, ...]:
        return (self.name, *self.aliases)


@dataclass(frozen=True)
class Mapping:
    """Which source key feeds each of our fields, and how we worked it out."""

    by_field: dict[str, str] = field(default_factory=dict)
    tier_by_field: dict[str, str] = field(default_factory=dict)
    unmapped: tuple[str, ...] = ()
    unmapped_identity: tuple[str, ...] = ()
    unused_source_keys: tuple[str, ...] = ()

    @property
    def usable(self) -> bool:
        """Every identity field found. Anything less must not be written."""
        return not self.unmapped_identity

    def apply(self, record: dict) -> dict:
        """Project one source record onto our field names."""
        out: dict[str, Any] = {}
        for name, key in self.by_field.items():
            value = record.get(key)
            out[name] = "" if value is None else str(value).strip()
        for name in self.unmapped:
            out[name] = ""
        return out

    def describe(self) -> str:
        parts = [f"{n}←{self.by_field[n]}({self.tier_by_field.get(n, '?')})" for n in self.by_field]
        if self.unmapped:
            parts.append(f"sin mapear: {', '.join(self.unmapped)}")
        return " · ".join(parts)


def resolve_mapping(specs: Sequence[FieldSpec], source_keys: Sequence[str]) -> Mapping:
    """Match our fields against the source's, exactly then normalised.

    Deterministic and free. Run this before considering the model: on the HCDN
    change it recovers five of the six fields on its own.
    """
    remaining = {str(k) for k in source_keys}
    by_norm: dict[str, list[str]] = {}
    for key in remaining:
        by_norm.setdefault(normalize_key(key), []).append(key)

    by_field: dict[str, str] = {}
    tier: dict[str, str] = {}

    for spec in specs:
        hit = next((c for c in spec.candidates() if c in remaining), None)
        if hit is not None:
            by_field[spec.name], tier[spec.name] = hit, "exact"
            remaining.discard(hit)
            continue
        for candidate in spec.candidates():
            options = [k for k in by_norm.get(normalize_key(candidate), []) if k in remaining]
            if len(options) == 1:
                by_field[spec.name], tier[spec.name] = options[0], "normalized"
                remaining.discard(options[0])
                break
            if len(options) > 1:
                # Two source keys that differ only in case or punctuation. Picking
                # one would be a coin flip, so neither is chosen and the field
                # falls through to the model or to a person.
                logger.warning(
                    "field mapping: %r matches %d source keys, refusing to guess",
                    spec.name,
                    len(options),
                )
                break

    unmapped = tuple(s.name for s in specs if s.name not in by_field)
    return Mapping(
        by_field=by_field,
        tier_by_field=tier,
        unmapped=unmapped,
        unmapped_identity=tuple(s.name for s in specs if s.identity and s.name not in by_field),
        unused_source_keys=tuple(sorted(remaining)),
    )


_LLM_SYSTEM = """Sos un asistente que mapea columnas de una fuente de datos.

Te doy los campos que necesito (con una descripción) y las columnas que sobraron
de la fuente, con valores de ejemplo. Decidí qué columna corresponde a cada campo
LEYENDO LOS VALORES, no el nombre.

Reglas:
- Si ninguna columna corresponde a un campo, no lo incluyas. Es mejor no mapear
  que mapear mal.
- Una columna se usa para un solo campo.
- Respondé SOLO JSON: {"mapping": {"campo": "columna_de_la_fuente"}}"""


async def propose_mapping_with_llm(
    specs: Sequence[FieldSpec],
    mapping: Mapping,
    sample_records: Sequence[dict],
    *,
    llm: Any,
    descriptions: dict[str, str] | None = None,
    max_rows: int = 5,
) -> Mapping:
    """Ask a model to place the fields the strings could not.

    Only the leftovers are offered — the fields still unmapped and the source
    keys still unused — so the model is never in a position to overrule a match
    that was already certain.

    Anything it proposes is checked against the leftovers before being accepted:
    a name that is not an unused source key is dropped rather than trusted. The
    caller is expected to have run the canary first; a model that cannot read a
    column of CUITs should not be reading these either.
    """
    import json

    if not mapping.unmapped or not mapping.unused_source_keys:
        return mapping

    descriptions = descriptions or {}
    samples: dict[str, list[str]] = {}
    for key in mapping.unused_source_keys:
        values = []
        for rec in sample_records[:max_rows]:
            v = rec.get(key)
            if v is not None and str(v).strip():
                values.append(str(v)[:80])
        samples[key] = values

    payload = {
        "campos_que_necesito": [
            {"campo": n, "descripcion": descriptions.get(n, "")} for n in mapping.unmapped
        ],
        "columnas_disponibles": samples,
    }

    from app.domain.ports.llm.llm_provider import LLMMessage

    try:
        response = await llm.chat_json(
            messages=[
                LLMMessage(role="system", content=_LLM_SYSTEM),
                LLMMessage(role="user", content=json.dumps(payload, ensure_ascii=False)),
            ],
            json_schema={
                "type": "object",
                "properties": {"mapping": {"type": "object"}},
                "required": ["mapping"],
            },
            temperature=0.0,
            max_tokens=1024,
        )
        raw = (response.content or "").strip()
        first, last = raw.find("{"), raw.rfind("}")
        proposed = json.loads(raw[first : last + 1]).get("mapping", {})
    except Exception:
        logger.warning("field mapping: the model could not be asked", exc_info=True)
        return mapping

    if not isinstance(proposed, dict):
        return mapping

    by_field = dict(mapping.by_field)
    tier = dict(mapping.tier_by_field)
    still_unused = set(mapping.unused_source_keys)
    for name, key in proposed.items():
        # Three ways a proposal is refused: a field we did not ask about, a
        # column that is not on offer, and a column already spent. Each of them
        # is a model answering a question it was not asked.
        if name not in mapping.unmapped or not isinstance(key, str):
            continue
        if key not in still_unused:
            continue
        by_field[name], tier[name] = key, "llm"
        still_unused.discard(key)

    return Mapping(
        by_field=by_field,
        tier_by_field=tier,
        unmapped=tuple(s.name for s in specs if s.name not in by_field),
        unmapped_identity=tuple(s.name for s in specs if s.identity and s.name not in by_field),
        unused_source_keys=tuple(sorted(still_unused)),
    )


# ── lo aprendido ───────────────────────────────────────────────
#
# A mapping the model worked out is worth exactly one call, and then it is worth
# nothing again next week unless it is written down. Remembering it turns the
# model tier from a recurring cost into a one-off: the next run resolves the
# same field deterministically, for free, and keeps working even if Bedrock is
# unreachable that morning.
#
# **The limitation, stated rather than hidden.** A learned alias is a guess that
# happened to work once. So it is only recorded after the batch it produced has
# been *used* — mapping resolved, identity present, content non-degenerate — and
# never from the mapping alone. That makes it evidence rather than confidence,
# and it is still weaker evidence than a name a person wrote in the spec: a
# model that mapped the wrong column onto plausible-looking values would be
# remembered as right. `forget_alias` exists for that, and the table records
# which tier put each row there so the mistakes are findable.

_ENSURE_ALIASES_SQL = text(
    """
    CREATE TABLE IF NOT EXISTS public.connector_field_aliases (
        connector   TEXT NOT NULL,
        field       TEXT NOT NULL,
        source_key  TEXT NOT NULL,
        tier        TEXT NOT NULL,
        learned_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        last_used_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        times_used  INTEGER NOT NULL DEFAULT 1,
        PRIMARY KEY (connector, field, source_key)
    )
    """
)

_LOAD_ALIASES_SQL = text(
    """
    SELECT field, source_key
    FROM public.connector_field_aliases
    WHERE connector = :connector
    ORDER BY times_used DESC, learned_at DESC
    """
)

_REMEMBER_SQL = text(
    """
    INSERT INTO public.connector_field_aliases (connector, field, source_key, tier)
    VALUES (:connector, :field, :source_key, :tier)
    ON CONFLICT (connector, field, source_key) DO UPDATE
        SET last_used_at = now(), times_used = public.connector_field_aliases.times_used + 1
    """
)


def learned_aliases(engine: Any, connector: str) -> dict[str, tuple[str, ...]]:
    """Source keys this connector has successfully used before, by field."""
    out: dict[str, list[str]] = {}
    try:
        with engine.begin() as conn:
            conn.execute(_ENSURE_ALIASES_SQL)
            for row in conn.execute(_LOAD_ALIASES_SQL, {"connector": connector}).fetchall():
                out.setdefault(row.field, []).append(row.source_key)
    except Exception:
        # Never let the memory break the connector. Without it the resolver
        # simply falls back to the spec's own aliases, which is where it started.
        logger.warning("field mapping: could not read learned aliases", exc_info=True)
        return {}
    return {k: tuple(v) for k, v in out.items()}


def with_learned(
    specs: Sequence[FieldSpec], learned: dict[str, tuple[str, ...]]
) -> tuple[FieldSpec, ...]:
    """The specs, widened by what this connector has learned.

    Learned keys go **after** the declared ones, so a name a person wrote always
    wins over a name a model proposed.
    """
    return tuple(
        FieldSpec(
            name=s.name,
            aliases=(*s.aliases, *(k for k in learned.get(s.name, ()) if k not in s.aliases)),
            identity=s.identity,
        )
        for s in specs
    )


def remember_mapping(engine: Any, connector: str, mapping: Mapping) -> int:
    """Record the mappings that were not already known. Returns how many.

    Call this only once the batch has proved itself. Recording at resolution
    time would remember a mapping that then turned out to produce nothing, which
    is precisely the failure the whole module exists to stop.
    """
    nuevos = [(f, k) for f, k in mapping.by_field.items() if mapping.tier_by_field.get(f) == "llm"]
    if not nuevos:
        return 0
    try:
        with engine.begin() as conn:
            conn.execute(_ENSURE_ALIASES_SQL)
            for field_name, key in nuevos:
                conn.execute(
                    _REMEMBER_SQL,
                    {
                        "connector": connector,
                        "field": field_name,
                        "source_key": key,
                        "tier": "llm",
                    },
                )
    except Exception:
        logger.warning("field mapping: could not remember the mapping", exc_info=True)
        return 0
    logger.info("field mapping: %s aprendió %s", connector, nuevos)
    return len(nuevos)


def forget_alias(engine: Any, connector: str, field_name: str, source_key: str) -> None:
    """Undo a learned alias. The escape hatch for a mapping that was wrong."""
    with engine.begin() as conn:
        conn.execute(
            text(
                "DELETE FROM public.connector_field_aliases "
                "WHERE connector = :c AND field = :f AND source_key = :k"
            ),
            {"c": connector, "f": field_name, "k": source_key},
        )
