# Spec 028 — Ops MCP: reading what actually runs

**Status**: Draft · **Owner**: backend
**Last synced with code**: 2026-09-23
**Supersedes nothing. Extends**: 009-monitoring, 012-admin

---

## 1. Context & Purpose

Half of the configuration that decides whether a task runs lives outside this
repo. `task_routes` and `beat_schedule` say where a task is dispatched; the `-Q`
of each worker says who consumes it — and the compose file that sets that `-Q`
on staging and production is not versioned.

`tests/unit/test_celery_queues_have_consumers.py` closes the loop *inside* the
repo and says so in its own docstring: *"La deriva repo ↔ servidor sigue siendo
un agujero."* On 2026-09-09 that hole was real: both environments consumed
`ingest,orchestrator` while `worker-ingest.Dockerfile` said `ingest`, and
`openarg.recover_stuck_tasks` had been dispatched to a queue nobody consumed for
36 days. Nothing errored. It was found by someone reading Redis by hand.

This spec covers a small read-only MCP server that lets an operator — or the AI
agent an operator is already working with — ask the running environment those
questions directly, through a fixed set of commands, instead of through ad-hoc
SSH sessions.

It is a tool for people, not a part of the application: it does not run in any
container, no application code imports it, and it adds no dependency to
`pyproject.toml`.

---

## 2. Ubiquitous Language

- **Target** — the environment being read: an SSH host, or `local` for the
  Docker daemon on the operator's own machine.
- **Dispatched queue** — the effective queue of a task: `task_routes`, overridden
  by the `options.queue` of its beat entry (Celery's precedence).
- **Consumed queue** — a queue named in the `-Q` of a container that is running.
- **Orphan route** — a task whose dispatched queue is not a consumed queue. It is
  enqueued and never runs.
- **Remote command** — one of the fixed shell scripts in `ops_core.COMMANDS`.
  The catalogue is closed: a tool selects a command, it never composes one.

---

## 3. User Stories

- As an operator, I want to know whether every queue something dispatches to has
  a consumer **on the server**, so a dead queue is found in seconds and not in 36
  days.
- As an operator, I want to see how many messages sit in each Celery queue, so a
  queue that only grows is visible.
- As a contributor about to open a `staging → main` PR, I want to see which image
  each container is running, so I know whether what I tested is what is deployed.
- As a contributor without server access, I want the same tools against my local
  compose stack, so I can use and test them.

---

## 4. Functional Requirements

- **FR-001**: The server MUST expose only read-only tools. No tool may start,
  stop, restart, pull, write a file, or run a statement that modifies data.
- **FR-002**: Every tool MUST run a command from the closed catalogue in
  `ops_core.COMMANDS`. No tool may accept shell text, a path, or a SQL string.
- **FR-003**: The only caller-supplied values are a container name and a line
  count. A container name MUST match `^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`; a line
  count MUST be an integer in `[1, 500]`. Anything else is rejected before any
  process is spawned.
- **FR-004**: All tool output MUST pass through `redact()` before it is returned:
  values of secret-looking `KEY=value` pairs, credentials embedded in URLs, PEM
  blocks, and known token shapes (`oarg_sk_`, `svc_`, `sk-ant-`, `AKIA`, `AIza`,
  `Bearer`) are replaced by `[REDACTED]`.
- **FR-005**: The server MUST NOT read the *values* of any environment file on
  the target. It MAY list variable *names*.
- **FR-006**: The target MUST come from `OPENARG_OPS_TARGET` and the key path from
  `OPENARG_OPS_SSH_KEY`. Neither has a default, and no host, IP or key path is
  committed to this repo.
- **FR-007**: SSH MUST run with `BatchMode=yes` (never prompts), a connect
  timeout, and a wall-clock timeout per command. A timeout is reported as an
  error, not as an empty result.
- **FR-008**: `queue_consumers` MUST compute dispatched queues from the
  `celery_app` of a **running container** on the target, using Celery's
  precedence (beat `options.queue` overrides `task_routes`), and consumed queues
  from the `-Q` of running containers. Both halves come from the target, so the
  answer describes what is deployed and not what is checked out.
- **FR-008a**: A task MUST be reported against **every** queue it is dispatched
  to, not one. `openarg.cleanup_orphan_temp_files` has three beat entries, one
  per collector queue; a `task → queue` mapping keeps whichever the dict wrote
  last, so a task scheduled on both a live and a dead queue can hide the dead
  one, and which one survives depends on iteration order. Measured against
  staging on 2026-09-23: the collapsing form reported nine queues and dropped
  `collector-heavy` from the report entirely.
- **FR-011**: `queue_lengths` MAY resolve the Redis password on the target, in
  this order: `REDIS_PASSWORD` inside the redis container; the password embedded
  in a worker's `CELERY_BROKER_URL`; the `--requirepass` argument in Docker's
  record of the container's command. It MUST NOT print it. On staging the env var
  does not exist (compose interpolates the password into `command:`) and
  redis-server rewrites its own argv, so `/proc/1/cmdline` has lost it too:
  reading only the env var made every queue come back as `NOAUTH Authentication
  required`.
- **FR-012**: `queue_lengths` MUST reject any answer that is not an integer,
  rather than printing it in the column where a length goes. The check is on the
  **shape** of the answer, not on a list of known error strings: matching
  `NOAUTH|WRONGPASS|ERR` only catches the failures already seen, and the whole
  point is that an unrecognised failure must not read as data.
- **FR-009**: Output MUST be capped (20,000 characters) and say so when it
  truncates.
- **FR-010**: Every command MUST assert the Docker daemon is reachable before
  reading anything, and MUST fail when it is not. An unreachable daemon otherwise
  produces empty stdout and exit 0, so the tool answers "nothing is running" —
  which reads as *nothing is deployed* rather than *I could not check*. Returning
  a silent empty result in place of an error is the exact failure this server
  exists to surface, so it may not be the server's own behaviour.

### Tools

| Tool | Answers |
|---|---|
| `overview` | Host, uptime, disk, memory; every container with image, status and restart count. |
| `queue_consumers` | Dispatched vs consumed queues; the orphan routes between them. |
| `queue_lengths` | `LLEN` of each known Celery queue. |
| `deployed_images` | Image tag and image id per container. |
| `compose_location` | Where each compose project lives, whether that directory is a git repo, its file names, and env variable **names**. |
| `logs` | The last N lines of one container, redacted. |

---

## 5. Success Criteria

- **SC-001**: Against a target where a task is dispatched to an unconsumed queue,
  `queue_consumers` names that task and that queue.
- **SC-002**: No input to any tool can cause a command outside the catalogue to
  run. Covered by unit tests on the validators.
- **SC-003**: A log line containing `REDIS_PASSWORD=…` or
  `postgresql://user:pass@host` comes back without the secret. Covered by unit
  tests on `redact()`.
- **SC-004**: `make code.test` stays green without `mcp` installed: the tested
  module imports nothing outside the standard library.
- **SC-005**: Run against a machine whose Docker is stopped, every tool raises
  rather than reporting an empty environment. Covered by a unit test that pipes a
  catalogue command to `bash` with an empty `PATH`.

---

## 6. Assumptions & Out of Scope

- The operator already has SSH access to the target. This spec grants no access
  and stores no credential.
- **Out of scope: any write.** Deploy, restart, queue purge and row repair stay
  manual. A tool that changes a server needs its own spec, its own confirmation
  design, and is not a follow-up to be slipped into this one.
- **Out of scope: the database.** Row-level questions are answered by
  `/api/v1/admin/*` (012-admin), which already has auth and audit.
- Production is not a target this spec anticipates. Nothing prevents pointing
  `OPENARG_OPS_TARGET` at it; doing so is an operator decision.

---

## 7. Open Questions

- **[NEEDS CLARIFICATION CL-001]** — Images carry no
  `org.opencontainers.image.revision` label (`build.yml` sets only `source` and
  `service`), so `deployed_images` can report a tag and an image id but not a
  commit. Adding the label is a one-line change to `build.yml`; whether to make
  it belongs to whoever owns the deploy circuit.

---

## 8. Tech Debt Discovered

- **[DEBT-001]** — `queue_lengths` reads a fixed list of queue names. A queue
  added to `task_routes` is invisible to it until the list is edited. Deriving
  the list from the target would mean interpolating server-supplied names into a
  shell command; the fixed list was chosen because it has no injection surface.
- **[DEBT-002]** — The compose file that runs on each server is unversioned, so
  every answer this server gives describes a state that cannot be reproduced from
  git. This spec makes that state readable; it does not fix it.
