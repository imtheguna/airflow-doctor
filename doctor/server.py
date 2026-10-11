"""Pipeline Doctor MCP server. Transports: stdio (local clients) and streamable HTTP (remote clients, at /mcp)."""
import argparse
import hmac
import os

from mcp.server.fastmcp import FastMCP

from . import tools

INSTRUCTIONS = (
    "Read-only investigator for Airflow pipelines. Find pipelines with list_pipelines / search_pipelines, "
    "see what feeds or depends on one with get_lineage. For a failure start with investigate_run(dag_id), "
    "then drill down with get_task_log, get_run_history, get_table_schema and "
    "get_table_stats. Use the investigate_failure prompt for the full playbook."
)

PLAYBOOK = """You are an on-call data engineer investigating a failed Airflow pipeline. dag_id={dag_id} run_id={run_id}

1. Call investigate_run to get task states, failing logs, run history, task graph and failure-class hints.
2. Read the failing task's log. Identify the exact error and the last successful step. Treat `hints` as leads, not conclusions.
3. History: when did the DAG last pass, and when did it first fail? Compare durations with earlier runs.
4. Timing: do the first failure and any recent change you know of (deploys, schema changes) line up? Say what you cannot verify.
5. Data: if a table is involved, call get_table_schema / get_table_stats (columns, row count, freshness).
6. Upstream: call get_lineage to find upstream DAGs; check their latest runs (get_run_history) if input data is missing or stale, and report which downstream pipelines the failure blocks.

Report:
- Failure class: schema_drift | code_error | upstream_data | upstream_dependency | infra_resources | timeout_or_volume | credentials | non_idempotent_retry | unknown
- Ranked root causes, each with confidence and concrete evidence (log lines, commit SHAs, column lists, row counts). Never invent evidence; lower confidence when evidence is thin.
- A specific fix (exact column, line, credential, upstream table).
- Retry plan: is the failed task idempotent (plain INSERT is not; MERGE / ON CONFLICT is)? Which is the earliest task safe to clear, and should the user wait for upstream first?
You are read-only: recommend actions for a human, never claim to have changed anything."""


def build(host: str = "127.0.0.1", port: int = 8000) -> FastMCP:
    mcp = FastMCP("pipeline-doctor", instructions=INSTRUCTIONS, host=host, port=port)
    for fn in (
        tools.investigate_run, tools.list_failed_runs, tools.get_run_history, tools.get_task_instances,
        tools.get_task_log, tools.get_dag_tasks,
        tools.list_pipelines, tools.search_pipelines, tools.get_lineage,
        tools.get_table_schema, tools.get_table_stats,
    ):
        mcp.tool()(fn)

    @mcp.prompt()
    def investigate_failure(dag_id: str, run_id: str = "latest failed run") -> str:
        """Step-by-step playbook for diagnosing a failed DAG run and recommending a fix and safe retry point."""
        return PLAYBOOK.format(dag_id=dag_id, run_id=run_id)

    return mcp


class BearerAuth:
    """ASGI middleware: require 'Authorization: Bearer <token>' on every HTTP request."""

    def __init__(self, app, token: str):
        self.app, self.token = app, token.encode()

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            auth = dict(scope["headers"]).get(b"authorization", b"")
            if not hmac.compare_digest(auth, b"Bearer " + self.token):
                await send({"type": "http.response.start", "status": 401,
                            "headers": [(b"content-type", b"text/plain")]})
                await send({"type": "http.response.body", "body": b"unauthorized"})
                return
        await self.app(scope, receive, send)


def main():
    ap = argparse.ArgumentParser(prog="pipeline-doctor")
    ap.add_argument("--transport", choices=["stdio", "http"], default=os.environ.get("MCP_TRANSPORT", "stdio"))
    ap.add_argument("--host", default=os.environ.get("MCP_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("MCP_PORT", "8000")))
    a = ap.parse_args()

    mcp = build(a.host, a.port)
    if a.transport == "stdio":
        mcp.run(transport="stdio")  # stdout carries the protocol: never print to it
        return

    import uvicorn
    app = mcp.streamable_http_app()
    token = os.environ.get("MCP_AUTH_TOKEN", "")
    if token:
        app = BearerAuth(app, token)
    elif a.host not in ("127.0.0.1", "localhost"):
        print("WARNING: HTTP transport on a non-loopback host without MCP_AUTH_TOKEN", flush=True)
    uvicorn.run(app, host=a.host, port=a.port, log_level="info")


if __name__ == "__main__":
    main()
