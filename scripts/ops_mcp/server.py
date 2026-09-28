# /// script
# requires-python = ">=3.12"
# dependencies = ["mcp>=2,<3"]
# ///
"""Read-only MCP server over a running OpenArg environment (spec 028).

    OPENARG_OPS_TARGET=ec2-user@<host> OPENARG_OPS_SSH_KEY=~/.ssh/<key> \
        uv run scripts/ops_mcp/server.py

`OPENARG_OPS_TARGET=local` reads the Docker daemon on this machine instead.
This is the only file that spawns a process; what may run is decided in
`ops_core`, which is the part under test.
"""

from __future__ import annotations

import json
import os
import subprocess

import ops_core
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from mcp.types import ToolAnnotations

_TIMEOUT_SECONDS = 60
_READ_ONLY = ToolAnnotations(read_only_hint=True, destructive_hint=False, idempotent_hint=True)

server = MCPServer(
    "openarg-ops",
    instructions=(
        "Read-only view of a running OpenArg environment: containers, Celery "
        "queues and their consumers, deployed images, compose location, logs. "
        "No tool changes anything on the target. Secrets are redacted."
    ),
)


def _run(command: str, *args: str) -> str:
    """Run one catalogue command on the target and return its redacted output.

    Failures are raised as `ToolError`: it is the only exception whose message
    the SDK forwards to the caller.
    """
    script = ops_core.COMMANDS[command]
    target = os.environ.get("OPENARG_OPS_TARGET", "").strip()
    if not target:
        raise ToolError("OPENARG_OPS_TARGET is not set (an SSH target, or 'local')")

    if target == "local":
        argv = ["bash", "-s", "--", *args]
    else:
        key = os.path.expanduser(os.environ.get("OPENARG_OPS_SSH_KEY", "").strip())
        if not key or not os.path.isfile(key):
            raise ToolError("OPENARG_OPS_SSH_KEY does not point to a key file")
        argv = [
            "ssh",
            "-i",
            key,
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=10",
            "-o",
            "StrictHostKeyChecking=accept-new",
            target,
            "bash",
            "-s",
            "--",
            *args,
        ]

    try:
        done = subprocess.run(  # noqa: S603 - argv is fixed; args are validated in ops_core
            argv, input=script, capture_output=True, text=True, timeout=_TIMEOUT_SECONDS
        )
    except subprocess.TimeoutExpired as exc:
        raise ToolError(f"'{command}' timed out after {_TIMEOUT_SECONDS}s") from exc
    if done.returncode != 0:
        detail = ops_core.redact(done.stderr.strip())[-500:]
        raise ToolError(f"'{command}' failed (exit {done.returncode}): {detail}")
    return ops_core.cap(ops_core.redact(done.stdout))


@server.tool(annotations=_READ_ONLY)
def overview() -> str:
    """Host, uptime, disk and memory, plus every container with its image, status and restart count."""
    return _run("overview")


@server.tool(annotations=_READ_ONLY)
def queue_consumers() -> str:
    """Compare the queues tasks are dispatched to against the queues running workers consume.

    Both halves are read from the target. `orphan_routes` lists every task that
    is enqueued to a queue nobody consumes: it is scheduled, it never runs, and
    nothing reports an error.
    """
    consumed = ops_core.parse_consumed_queues(_run("worker_commands"))
    try:
        report = ops_core.find_orphan_routes(_run("dispatched_routes"), consumed)
    except ValueError as exc:
        raise ToolError(str(exc)) from exc
    return json.dumps(report, indent=2, ensure_ascii=False)


@server.tool(annotations=_READ_ONLY)
def queue_lengths() -> str:
    """Number of pending messages in each known Celery queue. A queue that only grows has no consumer."""
    try:
        return ops_core.check_queue_lengths(_run("queue_lengths"))
    except ValueError as exc:
        raise ToolError(str(exc)) from exc


@server.tool(annotations=_READ_ONLY)
def deployed_images() -> str:
    """Image tag and image id each running container was started from."""
    return _run("deployed_images")


@server.tool(annotations=_READ_ONLY)
def compose_location() -> str:
    """Where each compose project lives, whether that directory is versioned, its files, and env variable NAMES (never values)."""
    return _run("compose_location")


@server.tool(annotations=_READ_ONLY)
def logs(container: str, lines: int = 100) -> str:
    """Last `lines` log lines (1-500) of one container, with secrets redacted."""
    try:
        args = (ops_core.validate_container(container), str(ops_core.validate_lines(lines)))
    except ValueError as exc:
        raise ToolError(str(exc)) from exc
    return _run("logs", *args)


if __name__ == "__main__":
    server.run("stdio")
