"""Read-only investigation tools. Docstrings are the tool descriptions shown to the MCP client."""
import functools
import re
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote

import httpx
import psycopg2
from psycopg2 import sql

from .config import settings

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SECRET = re.compile(r"(?i)(password|passwd|token|secret|api[_-]?key)(\s*[=:]\s*)\S+")

# (failure class, regex) -- cheap hints to anchor the client's reasoning, not a verdict.
SIGNATURES = [
    ("schema_drift", r"UndefinedColumn|column .{1,80} does not exist|UndefinedTable|relation .{1,80} does not exist|Unknown column|Invalid column"),
    ("non_idempotent_retry", r"UniqueViolation|duplicate key|Duplicate entry|IntegrityError"),
    ("timeout_or_volume", r"AirflowTaskTimeout|execution_timeout|Timeout, PID|timed out|statement timeout"),
    ("credentials", r"\b401\b|\b403\b|Unauthorized|Forbidden|permission denied|authentication failed|token_expired|access denied|ExpiredToken"),
    ("infra_resources", r"OutOfMemory|MemoryError|OOMKilled|Killed|SIGKILL|exit code -9|Worker lost|zombie|no space left|heartbeat"),
    ("upstream_dependency", r"Connection refused|could not connect|Name or service not known|Max retries exceeded|ConnectionError|Service Unavailable|Bad Gateway|\bHTTP[/ ]\S*\s*50[234]\b|\b50[234] (Server|Service|Bad|Gateway)"),
    ("upstream_data", r"\b0 rows\b|empty (table|dataframe|result)|no (data|rows|files) (found|available)|NoSuchKey|FileNotFound"),
    ("code_regression", r"ModuleNotFoundError|ImportError|SyntaxError|NameError|AttributeError|TypeError|KeyError|IndentationError"),
]


def _safe(fn):
    """Turn failures into short, readable errors the client can reason about."""
    @functools.wraps(fn)
    def wrapper(*a, **kw):
        try:
            return fn(*a, **kw)
        except httpx.HTTPStatusError as e:
            raise RuntimeError(f"Airflow API {e.response.status_code}: {e.response.text[:300]}") from None
        except httpx.RequestError as e:
            raise RuntimeError(f"Cannot reach Airflow at {settings.airflow_url}: {type(e).__name__}") from None
    return wrapper


def _clip(text: str, keep: str = "tail") -> str:
    text = _SECRET.sub(r"\1\2***", text)
    n = settings.max_output_chars
    if len(text) <= n:
        return text
    return "...[truncated]...\n" + text[-n:] if keep == "tail" else text[:n] + "\n...[truncated]..."


def _af(path: str, accept: str = "application/json", **params):
    r = httpx.get(
        f"{settings.airflow_url.rstrip('/')}/api/v1{path}",
        auth=(settings.airflow_user, settings.airflow_password) if settings.airflow_user else None,
        params={k: v for k, v in params.items() if v is not None},
        headers={"Accept": accept},
        timeout=30,
    )
    r.raise_for_status()
    return r


def _q(s: str) -> str:
    return quote(s, safe="")


def _run_row(r: dict) -> dict:
    return {k: r.get(k) for k in ("dag_run_id", "state", "start_date", "end_date", "run_type")}


@_safe
def list_failed_runs(dag_id: str = "~", limit: int = 5) -> list[dict]:
    """Most recent failed DAG runs, newest first. dag_id '~' means all DAGs."""
    runs = _af(f"/dags/{_q(dag_id)}/dagRuns", state="failed", order_by="-start_date", limit=limit).json()["dag_runs"]
    return [{"dag_id": r["dag_id"], **_run_row(r)} for r in runs]


@_safe
def get_run_history(dag_id: str, limit: int = 10) -> list[dict]:
    """Recent runs (any state) of a DAG, newest first. Use it to find when the DAG last passed and when failures began."""
    runs = _af(f"/dags/{_q(dag_id)}/dagRuns", order_by="-start_date", limit=limit).json()["dag_runs"]
    return [_run_row(r) for r in runs]


@_safe
def get_task_instances(dag_id: str, run_id: str) -> list[dict]:
    """Per-task state, try count and duration for one DAG run."""
    tis = _af(f"/dags/{_q(dag_id)}/dagRuns/{_q(run_id)}/taskInstances").json()["task_instances"]
    keys = ("task_id", "state", "try_number", "max_tries", "duration", "start_date", "end_date", "operator")
    return [{k: t.get(k) for k in keys} for t in tis]


@_safe
def get_task_log(dag_id: str, run_id: str, task_id: str, try_number: int = 1) -> str:
    """Log text for one task try (tail of the log, secrets redacted). If the try does not exist, try a lower number."""
    return _clip(_af(
        f"/dags/{_q(dag_id)}/dagRuns/{_q(run_id)}/taskInstances/{_q(task_id)}/logs/{try_number}",
        accept="text/plain",
    ).text)


@_safe
def get_dag_tasks(dag_id: str) -> list[dict]:
    """Task graph of a DAG: downstream tasks, retries and execution timeout per task."""
    tasks = _af(f"/dags/{_q(dag_id)}/tasks").json()["tasks"]
    keys = ("task_id", "operator_name", "downstream_task_ids", "retries", "execution_timeout")
    return [{k: t.get(k) for k in keys} for t in tasks]


def _db():
    if not settings.warehouse_dsn:
        raise RuntimeError("warehouse access not configured (WAREHOUSE_DSN is empty)")
    return psycopg2.connect(settings.warehouse_dsn)


def _check_ident(*names):
    for n in names:
        if n and not _IDENT.match(n):
            raise ValueError(f"invalid identifier: {n!r}")


def get_table_schema(schema: str, table: str) -> list[dict]:
    """Current column names and types of a Postgres warehouse table. Compare with the columns a failing query uses."""
    _check_ident(schema, table)
    with _db() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            "WHERE table_schema=%s AND table_name=%s ORDER BY ordinal_position", (schema, table))
        cols = [{"column": c, "type": t, "nullable": n} for c, t, n in cur.fetchall()]
    if not cols:
        raise RuntimeError(f"table {schema}.{table} not found")
    return cols


def get_table_stats(schema: str, table: str, freshness_column: str = "") -> dict:
    """Row count of a warehouse table, plus max of a timestamp column and the DB's now() to judge freshness."""
    _check_ident(schema, table, freshness_column)
    ident = sql.Identifier(schema, table)
    with _db() as conn, conn.cursor() as cur:
        if freshness_column:
            cur.execute(sql.SQL("SELECT count(*), max({}) FROM {}").format(sql.Identifier(freshness_column), ident))
            n, latest = cur.fetchone()
            cur.execute("SELECT now()")
            return {"row_count": n, f"max_{freshness_column}": str(latest), "now": str(cur.fetchone()[0])}
        cur.execute(sql.SQL("SELECT count(*) FROM {}").format(ident))
        return {"row_count": cur.fetchone()[0]}


def classify_log(text: str) -> list[dict]:
    """Regex failure-class hints for a log. Hints only; the client decides."""
    hits = []
    for cls, pat in SIGNATURES:
        m = re.search(pat, text, re.I)
        if m:
            hits.append({"class": cls, "matched": m.group(0)[:120]})
    return hits


# ---------------------------------------------------------------- discovery & lineage

def _paged(path: str, key: str, cap: int = 1000, page: int = 100, **params) -> tuple[list, int]:
    """Collect up to `cap` items from an Airflow list endpoint. Returns (items, total_entries)."""
    out, total = [], 0
    while len(out) < cap:
        batch = _af(path, limit=page, offset=len(out), **params).json()
        items = batch.get(key, [])
        total = batch.get("total_entries", total)
        out += items
        if len(items) < page or len(out) >= total:
            break
    return out[:cap], total


def _dag_row(d: dict) -> dict:
    sched = d.get("timetable_description") or (d.get("schedule_interval") or {}).get("value")
    return {
        "dag_id": d["dag_id"],
        "paused": d.get("is_paused"),
        "schedule": sched,
        "tags": [t["name"] for t in d.get("tags") or []],
        "owners": d.get("owners"),
        "description": d.get("description"),
        "file": d.get("fileloc"),
        "next_run": d.get("next_dagrun"),
    }


def _last_run(dag_id: str):
    try:
        runs = _af(f"/dags/{_q(dag_id)}/dagRuns", order_by="-start_date", limit=1).json()["dag_runs"]
        return _run_row(runs[0]) if runs else None
    except Exception:
        return None


def _tag_params(tags: str) -> dict:
    t = [x.strip() for x in tags.split(",") if x.strip()]
    return {"tags": t} if t else {}


@_safe
def list_pipelines(limit: int = 50, offset: int = 0, tags: str = "", include_paused: bool = True,
                   include_last_run: bool = True) -> dict:
    """List Airflow pipelines (DAGs) with schedule, tags, owners, paused flag and, optionally, the latest run's state.
    Comma-separated `tags` filters to DAGs carrying any of those tags. Page with limit/offset."""
    r = _af("/dags", limit=limit, offset=offset, order_by="dag_id", only_active=True, **_tag_params(tags)).json()
    rows = [_dag_row(d) for d in r["dags"] if include_paused or not d.get("is_paused")]
    if include_last_run:
        with ThreadPoolExecutor(8) as ex:
            for row, last in zip(rows, ex.map(lambda x: _last_run(x["dag_id"]), rows)):
                row["last_run"] = last
    return {"total": r.get("total_entries"), "returned": len(rows), "offset": offset, "pipelines": rows}


@_safe
def search_pipelines(query: str, tags: str = "", include_tasks: bool = False, limit: int = 25) -> dict:
    """Search pipelines by free text over dag_id, description, tags, owners and file path (every word must match).
    With include_tasks=true it also matches task ids and operator names (slower; scans up to 200 DAGs)."""
    terms = query.lower().split()
    dags, total = _paged("/dags", "dags", only_active=True, order_by="dag_id", **_tag_params(tags))
    hits = []

    def meta_hay(d):
        r = _dag_row(d)
        return " ".join([r["dag_id"], r["description"] or "", " ".join(r["tags"]), r["owners"] or "", r["file"] or ""]).lower()

    task_map = {}
    if include_tasks:
        def tasks_of(d):
            try:
                return d["dag_id"], _af(f"/dags/{_q(d['dag_id'])}/tasks").json()["tasks"]
            except Exception:
                return d["dag_id"], []
        with ThreadPoolExecutor(8) as ex:
            task_map = dict(ex.map(tasks_of, dags[:200]))

    for d in dags:
        hay, matched_tasks = meta_hay(d), []
        if include_tasks:
            th = [(t["task_id"], f"{t['task_id']} {t.get('operator_name') or ''}".lower()) for t in task_map.get(d["dag_id"], [])]
            matched_tasks = [tid for tid, h in th if all(x in h for x in terms)]
            hay += " " + " ".join(h for _, h in th)
        if all(x in hay for x in terms):
            row = _dag_row(d)
            if matched_tasks:
                row["matched_tasks"] = matched_tasks[:10]
            hits.append(row)

    q = query.lower()
    hits.sort(key=lambda r: (r["dag_id"].lower() != q, not r["dag_id"].lower().startswith(q), q not in r["dag_id"].lower(), r["dag_id"]))
    return {"query": query, "matches": len(hits), "searched_dags": min(len(dags), total) if total else len(dags),
            "pipelines": hits[:limit]}


_SRC_LINKS = [
    # (regex, direction) -- "waits_on": the DAG waits for target; "triggers": the DAG starts target
    (re.compile(r"external_dag_id\s*=\s*[\"']([\w.\-]+)[\"']"), "waits_on"),
    (re.compile(r"trigger_dag_id\s*=\s*[\"']([\w.\-]+)[\"']"), "triggers"),
]


def _source_links(dags: list[dict]) -> list[dict]:
    """Best-effort cross-DAG edges parsed from DAG source (ExternalTaskSensor / TriggerDagRunOperator)."""
    by_file: dict[str, list[str]] = {}
    for d in dags:
        if d.get("file_token"):
            by_file.setdefault(d["file_token"], []).append(d["dag_id"])

    def fetch(tok):
        try:
            return tok, _af(f"/dagSources/{tok}", accept="text/plain").text
        except Exception:
            return tok, ""

    with ThreadPoolExecutor(8) as ex:
        sources = dict(ex.map(fetch, by_file))
    edges = []
    for tok, text in sources.items():
        owners = by_file[tok]
        for rx, kind in _SRC_LINKS:
            for target in set(rx.findall(text)):
                for owner in owners:
                    src, dst = (target, owner) if kind == "waits_on" else (owner, target)
                    edges.append({"from": src, "to": dst, "via": "ExternalTaskSensor" if kind == "waits_on" else "TriggerDagRunOperator",
                                  "certainty": "file-level" if len(owners) > 1 else "source-parse"})
    return edges


def _node(name: str) -> str:
    return re.sub(r"\W", "_", name)


@_safe
def get_lineage(dag_id: str, scan_sources: bool = True) -> dict:
    """Lineage view for one pipeline. Returns (1) the task graph inside the DAG, (2) upstream/downstream DAGs linked via
    Airflow Datasets, and (3) with scan_sources=true, DAGs linked by ExternalTaskSensor / TriggerDagRunOperator parsed
    from DAG source (best effort, up to 300 DAGs). Includes a Mermaid diagram. Use it to find what feeds a failing pipeline
    and what a failure will block downstream."""
    tasks = get_dag_tasks(dag_id)
    task_edges = [{"from": t["task_id"], "to": d} for t in tasks for d in t.get("downstream_task_ids") or []]
    has_up = {e["to"] for e in task_edges}
    out = {
        "dag_id": dag_id,
        "tasks": [t["task_id"] for t in tasks],
        "task_edges": task_edges,
        "entry_tasks": [t["task_id"] for t in tasks if t["task_id"] not in has_up],
        "exit_tasks": [t["task_id"] for t in tasks if not t.get("downstream_task_ids")],
    }

    dag_edges, notes = [], []
    try:
        datasets, _ = _paged("/datasets", "datasets")
        for ds in datasets:
            producers = {p["dag_id"] for p in ds.get("producing_tasks") or []}
            consumers = {c["dag_id"] for c in ds.get("consuming_dags") or []}
            if dag_id in producers or dag_id in consumers:
                for a in producers:
                    for b in consumers:
                        if a != b:
                            dag_edges.append({"from": a, "to": b, "via": f"dataset {ds['uri']}", "certainty": "declared"})
    except Exception:
        notes.append("datasets API unavailable (needs Airflow >= 2.4)")

    if scan_sources:
        try:
            dags, total = _paged("/dags", "dags", cap=300, only_active=True)
            if total > 300:
                notes.append(f"scanned 300 of {total} DAGs; reverse links may be incomplete")
            dag_edges += [e for e in _source_links(dags) if dag_id in (e["from"], e["to"])]
        except Exception as e:
            notes.append(f"source scan failed: {e}")

    seen, uniq = set(), []
    for e in dag_edges:
        k = (e["from"], e["to"], e["via"])
        if k not in seen:
            seen.add(k)
            uniq.append(e)
    out["upstream_dags"] = [e for e in uniq if e["to"] == dag_id]
    out["downstream_dags"] = [e for e in uniq if e["from"] == dag_id]
    out["notes"] = notes

    lines = ["graph LR", f'  subgraph {_node(dag_id)}["{dag_id}"]']
    lines += [f"    {_node(dag_id)}__{_node(e['from'])}[{e['from']}] --> {_node(dag_id)}__{_node(e['to'])}[{e['to']}]" for e in task_edges]
    if not task_edges:
        lines += [f"    {_node(dag_id)}__{_node(t)}[{t}]" for t in out["tasks"]]
    lines.append("  end")
    def ref(name):  # the focus DAG is the subgraph itself; other DAGs are plain nodes
        return _node(dag_id) if name == dag_id else f'dag_{_node(name)}["{name}"]'

    lines += [f'  {ref(e["from"])} -->|{e["via"].split()[0]}| {ref(e["to"])}' for e in uniq]
    out["mermaid"] = "\n".join(lines)
    return out


@_safe
def investigate_run(dag_id: str, run_id: str = "", task_id: str = "") -> dict:
    """One-call evidence bundle for a failed run: task states, failing task logs, run history, task graph,
    dataset-based upstream/downstream DAGs and failure-class hints. Start here, then drill down with the other tools.
    Omit run_id to use the latest failed run. Pass task_id (the alerting task) to always include its log, even if
    Airflow has not yet recorded its state as failed."""
    if not run_id:
        failed = list_failed_runs(dag_id, 1)
        if not failed:
            raise RuntimeError(f"no failed runs found for {dag_id}")
        run_id = failed[0]["dag_run_id"]

    tasks = get_task_instances(dag_id, run_id)
    bundle = {"dag_id": dag_id, "run_id": run_id, "tasks": tasks, "failed_task_logs": {}, "hints": []}
    bundle["alert_task"] = task_id or None
    suspects = [t for t in tasks if t["state"] in ("failed", "up_for_retry")]
    if task_id and not any(t["task_id"] == task_id for t in suspects):
        # failure callbacks fire before the task's final state is committed, so trust the caller
        suspects.insert(0, next((t for t in tasks if t["task_id"] == task_id), {"task_id": task_id, "state": "unknown", "try_number": 1}))
    for t in suspects[:3]:
        log, n = None, max(1, t.get("try_number") or 1)
        while log is None and n >= 1:
            try:
                log = get_task_log(dag_id, run_id, t["task_id"], n)
            except RuntimeError:
                n -= 1
        log = log or "(log unavailable)"
        bundle["failed_task_logs"][t["task_id"]] = log
        bundle["hints"] += [{"task_id": t["task_id"], **h} for h in classify_log(log)]
    bundle["run_history"] = get_run_history(dag_id, 10)
    bundle["dag_tasks"] = get_dag_tasks(dag_id)
    try:  # datasets only (fast); get_lineage with scan_sources=true does the deeper source scan
        lin = get_lineage(dag_id, scan_sources=False)
        bundle["upstream_dags"], bundle["downstream_dags"] = lin["upstream_dags"], lin["downstream_dags"]
    except Exception as e:
        bundle["lineage"] = f"unavailable: {e}"
    return bundle
