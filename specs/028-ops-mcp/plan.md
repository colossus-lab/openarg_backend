# Plan 028 — Ops MCP

**Spec**: [./spec.md](./spec.md)
**Status**: Draft

Two files, split where the risk is. Everything that decides *what may run* is
pure and tested; the file that actually spawns a process is thin enough to read
in one sitting.

---

## Layout

```
scripts/ops_mcp/
├── ops_core.py    # pure, stdlib only: command catalogue, validators, redact(), parsers
├── server.py      # PEP 723 script: MCP wiring + the one function that spawns ssh/bash
└── README.md
tests/unit/test_ops_mcp_core.py
```

It lives under `scripts/` next to `diagnostics/` and `ci/` because it is operator
tooling. It is not under `src/app/`: no layer of the application depends on it,
and the hexagonal rules describe the application.

## Why `mcp` is not in `pyproject.toml`

`server.py` declares its one dependency inline (PEP 723), so
`uv run scripts/ops_mcp/server.py` resolves it in an isolated environment. The
application's lock file does not move, no image grows, and CI needs nothing new:
`ops_core.py` imports only the standard library, so its tests run in the existing
job (SC-004).

## The command catalogue

`ops_core.COMMANDS` maps a name to a fixed bash script. Scripts reach the target
on **stdin** (`ssh host bash -s -- <args>`), never as an argument string, so
there is no quoting layer to get wrong. The two caller-supplied values arrive as
positional parameters (`"$1"`, `"$2"`) and are validated first (FR-003); a
container name that passes the pattern contains no shell metacharacter.

`queue_consumers` needs the effective routing. Rather than importing the app
locally — which would describe the checkout, not the server — it runs a
six-line Python snippet inside a running worker container and prints JSON. That
snippet repeats the precedence rule of
`test_celery_queues_have_consumers._colas_despachadas` on purpose: same rule,
the other half of the comparison.

## Order of work

1. `ops_core.py` + unit tests (validators, `redact`, `-Q` parsing, orphan diff).
2. `server.py`, exercised end-to-end against `OPENARG_OPS_TARGET=local`.
3. `README.md` with the registration command.
4. ~~First run against staging~~ **DONE 2026-09-23.** All six tools ran against
   `staging.openarg.org`. It cost three bugs, each one a thing the local daemon
   could not have shown:
   - Docker unreachable produced an empty result and exit 0 (FR-010).
   - A task on several queues collapsed to one (FR-008a).
   - Redis auth: the password is in neither the container's env nor its argv
     (FR-011).

   Everything else held: zero orphan routes on the server, `worker_ingest`
   consuming `ingest,orchestrator`, and `compose_location` reporting
   `/opt/docker/openarg` as unversioned.

## Rollout

None. Nothing is deployed and nothing on a server changes. Removing the feature
is deleting the directory.
