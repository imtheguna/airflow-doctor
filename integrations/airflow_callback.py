"""Drop-in Airflow failure callback for Pipeline Doctor. Standard library only.

    from airflow_callback import notify_pipeline_doctor
    default_args = {"on_failure_callback": notify_pipeline_doctor}

Configure with environment variables on the Airflow scheduler/workers:
  DOCTOR_MODE         evidence (default) | agent | both
  DOCTOR_MCP_URL      e.g. http://pipeline-doctor:8000/mcp          (evidence mode)
  DOCTOR_MCP_TOKEN    bearer token if the MCP server sets MCP_AUTH_TOKEN
  DOCTOR_AGENT_URL    e.g. http://doctor-agent:8001/investigate     (agent mode)
  DOCTOR_AGENT_TOKEN  bearer token if the agent sets AGENT_AUTH_TOKEN
  NOTIFY_FILE         file that notifications are appended to (default: <tmpdir>/pipeline_doctor_notifications.log)

evidence: calls the MCP tool investigate_run and writes the evidence bundle to NOTIFY_FILE. No LLM involved.
agent:    pings the agent service, which runs Gemini over the MCP tools and writes a written diagnosis to its own notification file.
The callback never raises, so alerting can't fail or mask the task failure.
"""
import json
import logging
import os
import tempfile
import urllib.request
from datetime import datetime, timezone

log = logging.getLogger(__name__)
_JSON = {"Content-Type": "application/json", "Accept": "application/json, text/event-stream"}


def _post(url, payload, headers, timeout):
    req = urllib.request.Request(url, json.dumps(payload).encode(), headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.headers.get("mcp-session-id"), r.read().decode()


def _sse_json(body: str) -> dict:
    """MCP HTTP replies are either plain JSON or an SSE stream whose 'data:' line holds the JSON-RPC message."""
    for line in body.splitlines():
        if line.startswith("data:"):
            return json.loads(line[5:].strip())
    return json.loads(body)


def call_mcp_tool(url: str, tool: str, args: dict, token: str = "", timeout: int = 45):
    headers = dict(_JSON, **({"Authorization": f"Bearer {token}"} if token else {}))
    sid, _ = _post(url, {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
        "protocolVersion": "2025-06-18", "capabilities": {},
        "clientInfo": {"name": "airflow-callback", "version": "1"}}}, headers, timeout)
    if sid:
        headers["mcp-session-id"] = sid
    _post(url, {"jsonrpc": "2.0", "method": "notifications/initialized"}, headers, timeout)
    _, body = _post(url, {"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                          "params": {"name": tool, "arguments": args}}, headers, timeout)
    result = _sse_json(body).get("result", {})
    if result.get("isError"):
        raise RuntimeError(result["content"][0]["text"] if result.get("content") else "tool error")
    return result.get("structuredContent") or json.loads(result["content"][0]["text"])


def format_evidence(b: dict) -> str:
    failed = [t["task_id"] for t in b.get("tasks", []) if t.get("state") in ("failed", "up_for_retry")]
    alert = b.get("alert_task")
    if alert and alert not in failed:
        failed.insert(0, alert)  # callback fired before Airflow recorded the final state
    lines = [f"DAG {b['dag_id']} FAILED (run {b['run_id']})",
             "Failed tasks: " + (", ".join(failed) or "none found")]
    hints = b.get("hints") or []
    if hints:
        lines.append("Likely class: " + ", ".join(sorted({f"{h['class']} (matched '{h['matched'][:60]}')" for h in hints})))
    for tid, log_text in list((b.get("failed_task_logs") or {}).items())[:2]:
        tail = "\n".join("    " + l for l in log_text.strip().splitlines()[-8:])
        lines.append(f"Log tail for {tid}:\n{tail[:1500]}")
    hist = [r for r in b.get("run_history") or [] if r.get("dag_run_id") != b.get("run_id")]
    if hist:
        lines.append("Earlier runs (newest first): " + " ".join(r["state"] for r in hist[:10]))
    up = sorted({e["from"] for e in b.get("upstream_dags") or []})
    down = sorted({e["to"] for e in b.get("downstream_dags") or []})
    if up:
        lines.append("Upstream DAGs: " + ", ".join(up))
    if down:
        lines.append("Blocks downstream DAGs: " + ", ".join(down))
    return "\n".join(lines)


def write_notification(text: str, title: str = "evidence"):
    """Append one block to NOTIFY_FILE (created if missing)."""
    path = os.environ.get("NOTIFY_FILE") or os.path.join(tempfile.gettempdir(), "pipeline_doctor_notifications.log")
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(f"==== {stamp} | {title} ====\n{text}\n\n")


def notify_pipeline_doctor(context):
    ti = context["task_instance"]
    dag_id, run_id, task_id = ti.dag_id, context["run_id"], ti.task_id
    mode = os.environ.get("DOCTOR_MODE", "evidence").lower()
    if mode in ("agent", "both"):
        try:
            tok = os.environ.get("DOCTOR_AGENT_TOKEN", "")
            _post(os.environ["DOCTOR_AGENT_URL"], {"dag_id": dag_id, "run_id": run_id, "task_id": task_id},
                  {"Content-Type": "application/json", **({"Authorization": f"Bearer {tok}"} if tok else {})}, 5)
        except Exception:
            log.exception("pipeline-doctor: agent trigger failed")
    if mode in ("evidence", "both"):
        try:
            bundle = call_mcp_tool(os.environ["DOCTOR_MCP_URL"], "investigate_run",
                                   {"dag_id": dag_id, "run_id": run_id, "task_id": task_id},
                                   os.environ.get("DOCTOR_MCP_TOKEN", ""))
            write_notification(format_evidence(bundle), f"{dag_id} | evidence")
        except Exception:
            log.exception("pipeline-doctor: evidence collection failed")
