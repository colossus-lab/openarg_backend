"""Pure core of the ops MCP server (spec 028). Standard library only.

Everything that decides *what may run* lives here, so it can be tested without
`mcp` installed and without a server to talk to. `server.py` is the only file
that spawns a process.
"""

from __future__ import annotations

import json
import re

MAX_OUTPUT_CHARS = 20_000
MAX_LOG_LINES = 500

_CONTAINER_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")

# Fixed on purpose (spec 028 DEBT-001): deriving it from the target would mean
# interpolating server-supplied names into a shell command.
KNOWN_QUEUES = (
    "default",
    "scraper",
    "collector",
    "collector-heavy",
    "collector-heavy-retry",
    "embedding",
    "analyst",
    "transparency",
    "ingest",
    "orchestrator",
    "s3",
)

# Runs inside a worker container. Emits {task: [queues]} — a LIST, because one
# task can be dispatched to several queues and collapsing them hides orphans.
#
# `openarg.cleanup_orphan_temp_files` has three beat entries, one per collector
# queue (PR #61). A `dict[task] = queue` keeps only the last one, so if the task
# were scheduled on both a live queue and a dead one, whether the dead one is
# reported would depend on dict order. Measured against staging on 2026-09-23:
# the collapsing form reported 9 queues and hid `collector-heavy` entirely.
#
# Precedence is Celery's: a task with any beat entry is dispatched to that
# entry's queue, which overrides `task_routes`; a task with no beat entry uses
# `task_routes`. The override is per task, so all of its beat queues count.
_ROUTES_SNIPPET = (
    "import json;"
    "from app.infrastructure.celery.app import celery_app as a;"
    "routes=[(t,r['queue']) for t,r in (a.conf.task_routes or {}).items()"
    " if isinstance(r,dict) and r.get('queue')];"
    "beat=[(e['task'],(e.get('options') or {})['queue'])"
    " for e in (a.conf.beat_schedule or {}).values()"
    " if (e.get('options') or {}).get('queue')];"
    "scheduled={t for t,_ in beat};"
    "out={};"
    "[out.setdefault(t,set()).add(q) for t,q in"
    " [p for p in routes if p[0] not in scheduled]+beat];"
    "print(json.dumps({t:sorted(q) for t,q in out.items()}))"
)

# Every command reads the Docker daemon, so every command asserts it first.
# Without this, a daemon that cannot be reached yields empty stdout and exit 0:
# the command "succeeds" and reports nothing running, which an operator reads as
# "nothing is deployed" rather than "I could not check". That silent-empty answer
# is the exact shape of failure this server exists to catch — found on 2026-09-23
# by running `deployed_images` against a machine whose Docker was stopped.
_DOCKER_GUARD = 'docker info >/dev/null 2>&1 || { echo "cannot reach the Docker daemon on the target" >&2; exit 90; }\n'

# The closed catalogue (FR-002). Scripts reach the target on stdin; the only
# caller-supplied values are "$1" / "$2", validated before anything is spawned.
_RAW_COMMANDS: dict[str, str] = {
    "overview": r"""
echo "## host"; hostname; uname -sr; uptime
echo "## disk"; df -h / 2>/dev/null | tail -1
echo "## memory"; (free -h 2>/dev/null || vm_stat 2>/dev/null) | head -3
echo "## containers (name | image | status | restarts)"
for c in $(docker ps -a --format '{{.Names}}' | sort); do
  printf '%s | %s | %s | %s\n' "$c" \
    "$(docker inspect --format '{{.Config.Image}}' "$c")" \
    "$(docker inspect --format '{{.State.Status}}' "$c")" \
    "$(docker inspect --format '{{.RestartCount}}' "$c")"
done
""",
    "worker_commands": r"""
for c in $(docker ps --format '{{.Names}}' | sort); do
  printf '%s\t%s %s\n' "$c" \
    "$(docker inspect --format '{{join .Config.Entrypoint " "}}' "$c")" \
    "$(docker inspect --format '{{join .Config.Cmd " "}}' "$c")"
done
""",
    "dispatched_routes": (
        r"""
w="$(docker ps --format '{{.Names}}' | grep -E 'worker|beat' | sort | head -1)"
[ -n "$w" ] || { echo '{"error": "no running worker or beat container"}'; exit 0; }
docker exec "$w" python -c """
        + '"'
        + _ROUTES_SNIPPET
        + '"'
        + "\n"
    ),
    # Redis needs a password, and the target keeps it in one of two places.
    # First choice is `REDIS_PASSWORD` inside the container, which never leaves
    # it. Staging has no such env var — compose interpolates the password into
    # `command:` — and redis-server rewrites its own argv to `redis-server
    # *:6379`, so `/proc/1/cmdline` has lost it too. The fallback is Docker's
    # own record of the command it was started with.
    #
    # Measured against staging on 2026-09-23: reading only the env var made
    # every queue come back as `NOAUTH Authentication required`, printed in the
    # column where a length goes. The password is held in a shell variable on
    # the target and handed to the container as `-e RP`; it is never printed,
    # and `redact()` would catch it if it ever reached the output.
    "queue_lengths": (
        r"""
r="$(docker ps --format '{{.Names}}' | grep -i redis | sort | head -1)"
[ -n "$r" ] || { echo "no running redis container" >&2; exit 91; }
p="$(docker exec "$r" sh -c 'printf %s "${REDIS_PASSWORD:-}"')"
if [ -z "$p" ]; then
  w="$(docker ps --format '{{.Names}}' | grep -E 'worker|beat' | sort | head -1)"
  [ -n "$w" ] && p="$(docker exec "$w" printenv CELERY_BROKER_URL 2>/dev/null | sed -n 's|redis://[^:]*:\([^@]*\)@.*|\1|p')"
fi
[ -n "$p" ] || p="$(docker inspect --format '{{range .Config.Cmd}}{{println .}}{{end}}' "$r" | grep -A1 -x -- '--requirepass' | tail -1)"
docker exec -e RP="$p" "$r" sh -c '
  for q in """
        + " ".join(KNOWN_QUEUES)
        + r"""; do
    printf "%s\t%s\n" "$q" "$(redis-cli ${RP:+-a "$RP"} --no-auth-warning -n 0 LLEN "$q")"
  done
'
"""
    ),
    "deployed_images": r"""
for c in $(docker ps --format '{{.Names}}' | sort); do
  printf '%s | %s | %s\n' "$c" \
    "$(docker inspect --format '{{.Config.Image}}' "$c")" \
    "$(docker inspect --format '{{.Image}}' "$c" | cut -c1-19)"
done
""",
    "compose_location": r"""
for d in $(docker ps --format '{{.Label "com.docker.compose.project.working_dir"}}' | sort -u); do
  [ -n "$d" ] || continue
  echo "## $d"
  if git -C "$d" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "git: yes ($(git -C "$d" log -1 --format='%h %ad' --date=short 2>/dev/null))"
  else
    echo "git: NO - this directory is not versioned"
  fi
  echo "files:"; ls -1A "$d" 2>/dev/null | sed 's/^/  /'
  for f in "$d"/.env "$d"/.env.*; do
    [ -f "$f" ] || continue
    echo "variable names in $(basename "$f"):"
    grep -vE '^[[:space:]]*(#|$)' "$f" | cut -d= -f1 | sort | tr '\n' ' '; echo
  done
done
""",
    "logs": r"""
docker logs --tail "$2" "$1" 2>&1
""",
}

COMMANDS: dict[str, str] = {name: _DOCKER_GUARD + script for name, script in _RAW_COMMANDS.items()}


def validate_container(name: str) -> str:
    """Return *name* if it is a plausible container name, else raise (FR-003)."""
    if not isinstance(name, str) or not _CONTAINER_RE.fullmatch(name):
        raise ValueError(f"invalid container name: {name!r}")
    return name


def validate_lines(lines: int) -> int:
    """Return *lines* if it is an int in [1, MAX_LOG_LINES], else raise (FR-003)."""
    if isinstance(lines, bool) or not isinstance(lines, int):
        raise ValueError(f"lines must be an integer, got {lines!r}")
    if not 1 <= lines <= MAX_LOG_LINES:
        raise ValueError(f"lines must be between 1 and {MAX_LOG_LINES}, got {lines}")
    return lines


_SECRET_KEY = r"[A-Za-z0-9_]*(?:PASSWORD|PASSWD|SECRET|TOKEN|API_?KEY|ACCESS_KEY|PRIVATE_KEY|DSN)[A-Za-z0-9_]*"
_REDACTIONS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----", re.S),
        "[REDACTED PRIVATE KEY]",
    ),
    # scheme://user:password@host
    (re.compile(r"(\b[a-z][a-z0-9+.-]*://[^\s:/@]*):[^\s@/]+@"), r"\1:[REDACTED]@"),
    # KEY=value and "KEY": "value"
    (
        re.compile(rf"(?i)\b({_SECRET_KEY})(\s*=\s*)(\"[^\"]*\"|'[^']*'|[^\s,;]+)"),
        r"\1\2[REDACTED]",
    ),
    (re.compile(rf"(?i)(\"{_SECRET_KEY}\"\s*:\s*)\"[^\"]*\""), r'\1"[REDACTED]"'),
    (re.compile(r"(?i)\b(Bearer)\s+[A-Za-z0-9._~+/=-]{8,}"), r"\1 [REDACTED]"),
    (
        re.compile(r"\b(?:oarg_sk_|svc_|sk-ant-|ghp_|AKIA|AIza)[A-Za-z0-9_-]{8,}"),
        "[REDACTED]",
    ),
)


def redact(text: str) -> str:
    """Replace anything that looks like a secret with a placeholder (FR-004)."""
    for pattern, replacement in _REDACTIONS:
        text = pattern.sub(replacement, text)
    return text


def cap(text: str) -> str:
    """Bound tool output and say so when it truncates (FR-009)."""
    if len(text) <= MAX_OUTPUT_CHARS:
        return text
    dropped = len(text) - MAX_OUTPUT_CHARS
    return f"[truncated: {dropped} leading characters dropped]\n{text[-MAX_OUTPUT_CHARS:]}"


def check_queue_lengths(output: str) -> str:
    """Return *output* if every queue answered a number, else raise.

    A `LLEN` that answers anything else — `NOAUTH Authentication required.`, a
    connection error, an empty string — must never be printed in the column
    where a length goes. That is a silent-empty answer wearing a number's
    clothes, and an operator scanning the column reads it as data.

    Checking the shape rather than matching known error strings is deliberate:
    the first version matched `NOAUTH|WRONGPASS|ERR`, which only catches the
    failures already seen.
    """
    bad: list[str] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        queue, _, value = line.partition("\t")
        if not value.strip().lstrip("-").isdigit():
            bad.append(f"{queue.strip()} → {value.strip()[:60] or '(vacío)'}")
    if bad:
        raise ValueError("redis did not answer with a length: " + "; ".join(bad[:4]))
    return output


def parse_consumed_queues(worker_commands: str) -> dict[str, list[str]]:
    """Container -> queues it consumes, from `worker_commands` output.

    Each line is ``<container>\\t<entrypoint + cmd>``. A container with no `-Q`
    is not a Celery worker and is left out. `${VAR:-queue}` cannot appear here:
    these are the resolved arguments of a process that is already running.
    """
    consumed: dict[str, list[str]] = {}
    for line in worker_commands.splitlines():
        container, _, command = line.partition("\t")
        queues: list[str] = []
        for raw in re.findall(r"(?:-Q|--queues)[=\s]+([^\s]+)", command):
            queues.extend(q for q in raw.strip("\"'").split(",") if q)
        if container and queues:
            consumed[container] = queues
    return consumed


def find_orphan_routes(dispatched_json: str, consumed: dict[str, list[str]]) -> dict[str, object]:
    """Tasks dispatched to a queue that no running container consumes (FR-008).

    `dispatched_json` maps a task to the LIST of queues it is dispatched to; a
    task is reported once per dead queue, so a task scheduled on both a live and
    a dead queue still surfaces.
    """
    dispatched = json.loads(dispatched_json)
    if not isinstance(dispatched, dict) or "error" in dispatched:
        raise ValueError(f"could not read routes from the target: {dispatched_json.strip()[:200]}")
    live = {q for queues in consumed.values() for q in queues}
    orphans = {
        task: dead
        for task, queues in dispatched.items()
        if (dead := sorted(q for q in queues if q not in live))
    }
    return {
        "consumed_queues": sorted(live),
        "dispatched_queues": sorted({q for queues in dispatched.values() for q in queues}),
        "consumers": consumed,
        "orphan_routes": dict(sorted(orphans.items())),
        # A queue with a worker but nothing routed to it: not a failure, but it
        # means a container is paying for a queue nothing feeds.
        "consumed_but_never_dispatched": sorted(
            live - {q for queues in dispatched.values() for q in queues}
        ),
    }
