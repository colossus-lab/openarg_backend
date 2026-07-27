"""Everything baked into an image must be able to trigger a rebuild.

`build.yml` only fires on a `paths:` allowlist. `config/**` was missing
from it, while every Dockerfile carries `COPY config/ config/` and only
`config/prod` is bind-mounted at runtime. So the mart definitions — the
SQL that produces what users are served — reached containers exclusively
through a rebuild that a mart change could not trigger.

Observed 2026-07-27: PR #29 fixed a mart filter that was discarding 42 %
of the source rows. The merge ran the test suite and produced no image.
A deploy at that point would have pulled the previous tag and rebuilt the
mart from the old SQL, and nothing in CI, the deploy, or the mart build
would have reported a problem — the failure mode is silence.

This test is structural because the alternative is noticing.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

_ROOT = Path(__file__).resolve().parents[2]
_WORKFLOW = _ROOT / ".github" / "workflows" / "build.yml"
_DOCKER_DIR = _ROOT / "docker"

# Documentation copied for image provenance only: its content cannot change
# what a container computes, so a rebuild on every edit would be noise.
_EXEMPT = {"README.md"}

_COPY_LINE = re.compile(r"^\s*COPY\s+(?P<args>.+)$", re.IGNORECASE)


def _copied_sources(dockerfile: Path) -> set[str]:
    """Build-context paths a Dockerfile copies, excluding the destination.

    `COPY --from=<stage>` reads from an earlier build stage, not from the
    repository, so its absolute paths say nothing about which files should
    trigger a rebuild.
    """
    sources: set[str] = set()
    for line in dockerfile.read_text(encoding="utf-8").splitlines():
        match = _COPY_LINE.match(line)
        if not match:
            continue
        raw_args = match.group("args").split()
        if any(a.startswith("--from=") for a in raw_args):
            continue
        args = [a for a in raw_args if not a.startswith("--")]
        if len(args) < 2:
            continue
        for src in args[:-1]:  # last arg is the destination
            if src.startswith("/"):
                continue
            sources.add(src.rstrip("/"))
    return sources


def _build_trigger_paths() -> list[str]:
    workflow = yaml.safe_load(_WORKFLOW.read_text(encoding="utf-8"))
    # `on:` parses as the boolean True in YAML 1.1 unless quoted.
    triggers = workflow.get("on") or workflow.get(True)
    return list(triggers["push"]["paths"])


def _covers(patterns: list[str], source: str) -> bool:
    return any(p == source or p.startswith(f"{source}/") for p in patterns)


def _dockerfiles() -> list[Path]:
    return sorted(_DOCKER_DIR.glob("*.Dockerfile"))


class TestBuildTriggersCoverImageContents:
    def test_there_are_dockerfiles_to_check(self) -> None:
        """Guard against the parametrised test passing on an empty set."""
        assert _dockerfiles()

    @pytest.mark.parametrize("dockerfile", _dockerfiles(), ids=lambda p: p.name)
    def test_every_copied_path_can_trigger_a_build(self, dockerfile: Path) -> None:
        patterns = _build_trigger_paths()
        missing = sorted(
            src
            for src in _copied_sources(dockerfile)
            if src not in _EXEMPT and not _covers(patterns, src)
        )
        assert not missing, (
            f"{dockerfile.name} copies {missing} into the image, but no `paths:` entry "
            f"in build.yml matches — changing those files merges without producing an "
            f"image, and the next deploy silently ships the previous build. Add them to "
            f"the filter or to _EXEMPT with a reason."
        )

    def test_config_is_covered(self) -> None:
        """Regression: the specific gap that shipped a stale mart."""
        assert _covers(_build_trigger_paths(), "config")
