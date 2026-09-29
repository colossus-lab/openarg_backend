# Ops MCP

Read-only MCP server over a running OpenArg environment. Spec:
[`specs/028-ops-mcp/`](../../specs/028-ops-mcp/spec.md).

It answers the questions that today take an SSH session and someone who knows
where to look: is every queue something dispatches to actually consumed, how
many messages are waiting, which image is each container running, and where does
the compose file that runs live.

**It changes nothing on the target.** Every tool runs a fixed script from
`ops_core.COMMANDS`; the only values a caller supplies are a container name and a
line count, both validated before any process is spawned. All output is redacted.

## Tools

| Tool | Answers |
|---|---|
| `overview` | Host, uptime, disk, memory; every container with image, status, restarts. |
| `queue_consumers` | Dispatched vs consumed queues, and the tasks routed to a queue nobody consumes. |
| `queue_lengths` | `LLEN` of each known Celery queue. |
| `deployed_images` | Image tag and image id per running container. |
| `compose_location` | Where each compose project lives, whether it is versioned, its files, env variable **names**. |
| `logs` | Last N lines (1–500) of one container. |

## Run it

Needs [`uv`](https://docs.astral.sh/uv/). The one dependency (`mcp`) is declared
inline in `server.py`, so nothing is added to the project's environment.

```bash
# Against the Docker daemon on your own machine — no server access needed:
OPENARG_OPS_TARGET=local uv run scripts/ops_mcp/server.py

# Against a server you already have SSH access to:
OPENARG_OPS_TARGET=ec2-user@<host> OPENARG_OPS_SSH_KEY=~/.ssh/<key> \
  uv run scripts/ops_mcp/server.py
```

Register it in Claude Code:

```bash
claude mcp add openarg-ops \
  --env OPENARG_OPS_TARGET=ec2-user@<host> \
  --env OPENARG_OPS_SSH_KEY=$HOME/.ssh/<key> \
  -- uv run "$(pwd)/scripts/ops_mcp/server.py"
```

No host, IP or key path is committed to this repo, and neither variable has a
default: the server refuses to run without them.

## What it does not do

No restart, no deploy, no queue purge, no SQL. Those stay manual on purpose; a
tool that changes a server needs its own spec (see spec 028 §6).
