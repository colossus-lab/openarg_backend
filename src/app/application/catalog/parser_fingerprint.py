"""A version for the parser that the parser cannot forget to bump.

`OPENARG_PARSER_VERSION` is set to the literal string `2026-05-04` in staging,
and the collector has faithfully recorded it 21,989 times. The mechanism was
never broken; the value simply carries no information, because it only changes
when a person edits an environment file and nobody ever does.

That is not a small annoyance. G1 — the gate that asks *did our own parser
change?* — is the only gate with a producer, and on 2026-08-21 it fired zero
times against five findings that were all our parser's doing. A provenance value
that cannot distinguish a parser change from no change makes the cascade blame
the portals for our regressions.

So derive it from the parsers themselves.

**Structure, not bytes.** The fingerprint is taken over each module's AST with
docstrings stripped, so reformatting, renaming a local, or rewriting a comment
leaves it alone, while changing a literal, a branch, or a threshold moves it.
Hashing the file bytes would make every comment edit look like a behaviour
change and produce exonerations nobody earned — which is a worse failure than
the one being fixed, because it would be invisible.

The set of modules is explicit rather than discovered. A glob would silently
widen when someone adds a file, and the fingerprint would move for reasons no
one could reconstruct months later.
"""

from __future__ import annotations

import ast
import hashlib
import logging
from functools import lru_cache
from pathlib import Path

logger = logging.getLogger(__name__)

# Modules whose contents decide the *shape* of a parsed table. Listed by hand:
# a glob would quietly take in every new file in these packages, and a
# fingerprint that moves for unreconstructable reasons is no better than a date.
_PARSER_MODULES: tuple[str, ...] = (
    "app.application.pipeline.parsers.column_normalization",
    "app.application.pipeline.parsers.header_recovery",
    "app.application.pipeline.parsers.hierarchical_headers",
    "app.application.pipeline.parsers.time_pivot",
    "app.application.pipeline.parsers.pdf",
)

# Repairs rewrite a table's columns after the fact, so a change to them changes
# the shape a resource ends up with just as surely as a change to the parser.
# Tracked separately because a repair moving is a different event from a parser
# moving, and G1 should be able to tell an operator which one happened.
_NORMALIZATION_MODULES: tuple[str, ...] = (
    "app.application.repair.parse_repair",
    "app.application.pipeline.parsers.column_normalization",
)

_PREFIX_PARSER = "p"
_PREFIX_NORMALIZATION = "n"
_DIGEST_CHARS = 12

# Recognisable as "we could not compute one" rather than as a real version, so a
# consumer never mistakes a failure for a parser that happens to hash to this.
UNKNOWN = "unavailable"


def _module_path(dotted: str) -> Path | None:
    """Locate a module's source without importing it.

    Importing would pull the application layer's dependencies into whatever
    process asks for a fingerprint — including the Celery workers at startup,
    where an import cycle would be a fine way to break ingestion for a
    bookkeeping value.
    """
    root = Path(__file__).resolve().parents[3]  # .../src
    candidate = root.joinpath(*dotted.split(".")).with_suffix(".py")
    return candidate if candidate.is_file() else None


def _structural_digest(source: str) -> str:
    """Hash what the module *does*, ignoring how it reads.

    Docstrings are removed before dumping the tree. This codebase writes long
    explanatory docstrings and edits them often; if every such edit moved the
    fingerprint, G1 would exonerate changes that nothing actually caused.
    """
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if not isinstance(
            node, ast.Module | ast.ClassDef | ast.FunctionDef | ast.AsyncFunctionDef
        ):
            continue
        body = node.body
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            node.body = body[1:] or [ast.Pass()]
    return ast.dump(tree, annotate_fields=True, include_attributes=False)


def _fingerprint(modules: tuple[str, ...], prefix: str) -> str:
    digest = hashlib.sha256()
    seen = 0
    for dotted in sorted(modules):
        path = _module_path(dotted)
        if path is None:
            # A module that moved or was deleted is a real change to how parsing
            # works, so it must move the fingerprint rather than be skipped —
            # but it is also a packaging problem worth saying out loud.
            logger.warning("parser fingerprint: %s has no source on disk", dotted)
            digest.update(f"{dotted}:missing".encode())
            continue
        try:
            digest.update(dotted.encode())
            digest.update(_structural_digest(path.read_text(encoding="utf-8")).encode())
            seen += 1
        except (OSError, SyntaxError):
            logger.warning("parser fingerprint: could not read %s", dotted, exc_info=True)
            digest.update(f"{dotted}:unreadable".encode())

    if seen == 0:
        # Every module unreadable means the value would describe nothing. Saying
        # so beats emitting a confident hash of five error markers.
        return UNKNOWN
    return f"{prefix}:{digest.hexdigest()[:_DIGEST_CHARS]}"


@lru_cache(maxsize=1)
def parser_fingerprint() -> str:
    """Version of the code that decides a parsed table's shape. e.g. `p:1a2b3c4d5e6f`."""
    return _fingerprint(_PARSER_MODULES, _PREFIX_PARSER)


@lru_cache(maxsize=1)
def normalization_fingerprint() -> str:
    """Version of the code that rewrites columns after parsing. e.g. `n:9f8e7d6c5b4a`."""
    return _fingerprint(_NORMALIZATION_MODULES, _PREFIX_NORMALIZATION)


def is_real_provenance(value: str | None) -> bool:
    """Is this a value G1 can reason about, or a placeholder wearing its clothes?

    The corpus is full of values that look like provenance and are not:
    `legacy:unknown` from the catalogue backfill, and the bare date
    `2026-05-04` that `OPENARG_PARSER_VERSION` has been supplying. Counting
    those as provenance made coverage read 26,435 when the number G1 could use
    was zero.
    """
    if not value or value == UNKNOWN or value == "legacy:unknown":
        return False
    # A bare ISO date is what the environment variable has been supplying, and
    # it says nothing about which parser ran.
    parts = value.split("-")
    if len(parts) == 3 and all(p.isdigit() for p in parts):
        return False
    return True
