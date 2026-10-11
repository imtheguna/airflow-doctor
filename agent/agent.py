"""Pipeline Doctor agent: Gemini investigating failures through the Pipeline Doctor MCP server.

  serve:    python agent.py                      -> POST /investigate {dag_id, run_id?, task_id?} (queued, one at a time)
  one-shot: python agent.py --dag my_dag [--run-id RUN]
Each result is appended to NOTIFY_FILE and kept at GET /reports.
"""
import argparse
import asyncio
import contextlib
import hmac
import json
import os
import time
from collections import deque
from datetime import datetime, timezone

import httpx
import uvicorn
from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

# Comma-separated, in order of preference. When a model's daily quota is used up the agent moves to the next one.
MODELS = [m.strip() for m in os.environ.get("GEMINI_MODEL", "gemini-3-flash-preview,gemini-flash-latest").split(",") if m.strip()]
EXHAUSTED: dict[str, float] = {}  # model -> time until which we skip it
EXHAUSTED_FOR = 6 * 3600
API_KEY = os.environ.get("GEMINI_API_KEY", "")
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
MCP_URL = os.environ.get("MCP_URL", "http://localhost:8000/mcp")
MCP_TOKEN = os.environ.get("MCP_AUTH_TOKEN", "")
AGENT_TOKEN = os.environ.get("AGENT_AUTH_TOKEN", "")
NOTIFY_FILE = os.environ.get("NOTIFY_FILE", "/notifications/pipeline_doctor.log")
MAX_STEPS = int(os.environ.get("MAX_STEPS", "15"))

SYSTEM = (
    "You are Pipeline Doctor, an on-call data engineer. You have read-only tools for Airflow and the warehouse. "
    "Follow the investigation playbook in the user's message. Be efficient: call independent tools together. "
    "Finish with a concise report (under 350 words) in plain text: one-line verdict, failure class, ranked root "
    "causes each with the evidence you retrieved, the specific fix, the retry plan (is the task idempotent, which "
    "task to clear from) and any downstream pipelines affected. Never state evidence you did not retrieve."
)

REPORTS: deque = deque(maxlen=20)


def _text(result) -> str:
    return "\n".join(c.text for c in result.content if getattr(c, "type", "") == "text")


class QuotaExhausted(RuntimeError):
    def __init__(self, model: str, cooldown: float = EXHAUSTED_FOR, detail: str = ""):
        super().__init__(f"{model}: {detail or 'quota exhausted'}")
        self.model, self.cooldown = model, cooldown


def quota_info(text: str) -> tuple[list[str], float | None]:
    """(quota ids, retryDelay seconds) from a Gemini 429 body."""
    try:
        details = json.loads(text)["error"].get("details", [])
    except Exception:
        return [], None
    ids = [v.get("quotaId", "") for d in details if "QuotaFailure" in d.get("@type", "") for v in d.get("violations", [])]
    delay = next((d.get("retryDelay") for d in details if "RetryInfo" in d.get("@type", "")), None)
    try:
        return ids, float(str(delay).rstrip("s")) if delay else None
    except ValueError:
        return ids, None


def pick_model() -> str:
    now = time.time()
    for m in MODELS:
        if EXHAUSTED.get(m, 0) <= now:
            return m
    raise RuntimeError("Gemini daily quota exhausted for all configured models: " + ", ".join(MODELS)
                       + ". Add more models to GEMINI_MODEL or wait for the daily reset.")


async def gemini(http: httpx.AsyncClient, model: str, contents: list, declarations: list) -> dict:
    """One Gemini generateContent call with function calling. Returns the model's content (echo it back unchanged:
    Gemini 3 needs its thought signatures preserved across tool turns)."""
    body = {"systemInstruction": {"parts": [{"text": SYSTEM}]}, "contents": contents,
            "tools": [{"functionDeclarations": declarations}]}
    for attempt in range(5):
        r = await http.post(GEMINI_URL.format(model=model), headers={"x-goog-api-key": API_KEY}, json=body)
        if r.status_code == 429:
            ids, delay = quota_info(r.text)
            if any("PerDay" in i for i in ids):
                raise QuotaExhausted(model, EXHAUSTED_FOR, "daily quota exhausted")
            if delay is not None and delay <= 90 and attempt < 4:  # per-minute limit: wait as told, then retry
                await asyncio.sleep(delay + 1)
                continue
            if delay is None and attempt < 4:
                await asyncio.sleep(2 ** attempt * 2)
                continue
            raise QuotaExhausted(model, min(delay or 120, EXHAUSTED_FOR), f"rate limited ({', '.join(ids) or '429'})")
        if r.status_code in (500, 503) and attempt < 4:
            await asyncio.sleep(2 ** attempt * 2)
            continue
        if r.status_code != 200:
            raise RuntimeError(f"Gemini API error {r.status_code}: {r.text[:300]}")
        cands = r.json().get("candidates") or []
        if not cands or "content" not in cands[0]:
            reason = cands[0].get("finishReason") if cands else r.json().get("promptFeedback")
            raise RuntimeError(f"Gemini returned no content ({reason})")
        return cands[0]["content"]
    raise RuntimeError("Gemini unavailable after retries")


async def investigate(dag_id: str, run_id: str = "", task_id: str = "", log=print) -> str:
    headers = {"Authorization": f"Bearer {MCP_TOKEN}"} if MCP_TOKEN else None
    async with streamablehttp_client(MCP_URL, headers=headers) as (read, write, _):
        async with ClientSession(read, write) as mcp, httpx.AsyncClient(timeout=120) as http:
            await mcp.initialize()
            decls = [{"name": t.name, "description": t.description or "", "parametersJsonSchema": t.inputSchema}
                     for t in (await mcp.list_tools()).tools]
            prompt = await mcp.get_prompt("investigate_failure", {"dag_id": dag_id, **({"run_id": run_id} if run_id else {})})
            first = "\n".join(m.content.text for m in prompt.messages if getattr(m.content, "type", "") == "text")
            if task_id:
                first += f"\n(The alert came from task: {task_id})"
            start = [{"role": "user", "parts": [{"text": first}]}]
            contents, model, step = list(start), pick_model(), 0

            while step < MAX_STEPS:
                try:
                    content = await gemini(http, model, contents, decls)
                except QuotaExhausted as q:
                    EXHAUSTED[model] = time.time() + q.cooldown
                    log(f"  {q}; switching model")
                    model = pick_model()  # raises if every model is used up
                    contents = list(start)  # restart: thought signatures are tied to the model that produced them
                    continue
                step += 1
                contents.append(content)
                parts = content.get("parts", [])
                calls = [p["functionCall"] for p in parts if "functionCall" in p]
                if not calls:
                    text = "\n".join(p["text"] for p in parts if p.get("text") and not p.get("thought")).strip()
                    return (text or "(empty report)") + f"\n\n[model: {model}]"

                async def run(c):
                    log(f"  [{step}] {c['name']}({json.dumps(c.get('args') or {})[:120]})")
                    try:
                        r = await mcp.call_tool(c["name"], c.get("args") or {})
                        out = {"error": _text(r)} if r.isError else {"result": _text(r) or "(no output)"}
                    except Exception as e:
                        out = {"error": f"tool failed: {e}"}
                    fr = {"name": c["name"], "response": out}
                    if c.get("id"):
                        fr["id"] = c["id"]
                    return {"functionResponse": fr}

                contents.append({"role": "user", "parts": list(await asyncio.gather(*(run(c) for c in calls)))})
    return f"Investigation did not finish within {MAX_STEPS} steps."


def root_cause(e: BaseException) -> BaseException:
    """MCP's task groups wrap errors in ExceptionGroups; surface the real one."""
    while isinstance(e, BaseExceptionGroup) and e.exceptions:
        e = e.exceptions[0]
    return e


def write_notification(text: str):
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    os.makedirs(os.path.dirname(os.path.abspath(NOTIFY_FILE)), exist_ok=True)
    with open(NOTIFY_FILE, "a", encoding="utf-8") as f:
        f.write(f"==== {stamp} ====\n{text}\n\n")


async def worker(queue: asyncio.Queue):
    while True:
        job = await queue.get()
        title = f"Pipeline Doctor agent | DAG {job['dag_id']} | run {job.get('run_id') or 'latest failed'}"
        try:
            report = await investigate(job["dag_id"], job.get("run_id", ""), job.get("task_id", ""))
            REPORTS.appendleft({**job, "report": report})
            await asyncio.to_thread(write_notification, f"{title}\n{report}")
        except BaseException as e:
            if isinstance(e, asyncio.CancelledError):
                raise
            e = root_cause(e)
            print(f"investigation failed for {job}: {e}", flush=True)
            with contextlib.suppress(Exception):
                await asyncio.to_thread(write_notification, f"{title}\ninvestigation failed: {type(e).__name__}: {e}")


def build_app() -> Starlette:
    queue: asyncio.Queue = asyncio.Queue()

    @contextlib.asynccontextmanager
    async def lifespan(app):
        task = asyncio.create_task(worker(queue))
        yield
        task.cancel()

    async def post_investigate(request):
        if AGENT_TOKEN and not hmac.compare_digest(request.headers.get("authorization", ""), f"Bearer {AGENT_TOKEN}"):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        try:
            body = await request.json()
            dag_id = body["dag_id"]
        except Exception:
            return JSONResponse({"error": "JSON body with dag_id required"}, status_code=400)
        await queue.put({k: body.get(k, "") for k in ("dag_id", "run_id", "task_id")} | {"dag_id": dag_id})
        return JSONResponse({"accepted": True, "queued": queue.qsize()}, status_code=202)

    async def reports(request):
        return JSONResponse(list(REPORTS))

    async def health(request):
        return JSONResponse({"ok": True, "models": MODELS, "exhausted": [m for m, t in EXHAUSTED.items() if t > time.time()]})

    return Starlette(routes=[Route("/investigate", post_investigate, methods=["POST"]),
                             Route("/reports", reports), Route("/health", health)], lifespan=lifespan)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dag")
    ap.add_argument("--run-id", default="")
    ap.add_argument("--host", default=os.environ.get("AGENT_HOST", "127.0.0.1"))
    ap.add_argument("--port", type=int, default=int(os.environ.get("AGENT_PORT", "8001")))
    a = ap.parse_args()
    if a.dag:
        try:
            print(asyncio.run(investigate(a.dag, a.run_id)))
        except BaseException as e:
            raise SystemExit(f"investigation failed: {type(root_cause(e)).__name__}: {root_cause(e)}")
    else:
        uvicorn.run(build_app(), host=a.host, port=a.port)
