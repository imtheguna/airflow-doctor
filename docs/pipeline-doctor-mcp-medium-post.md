# I Built an On-Call Data Engineer for Airflow with MCP and Gemini

*A read-only MCP server that investigates failed pipelines, plus an optional Gemini agent that writes the diagnosis. Full setup, step by step.*

---

It is 2 a.m. An Airflow task has failed. You open the UI, click into the task, scroll through 400 lines of logs, check whether the DAG ever passed, wonder whether someone changed a table, and finally find a single line: `column "user_id" does not exist`.

That investigation is almost always the same loop: read the log, check the history, look at what the pipeline depends on, decide whether it is safe to retry. It is repetitive, and it is exactly the kind of work an AI assistant can do well, as long as it can see the same things you can.

So I built **Pipeline Doctor**: a small MCP server that gives an AI client read-only access to your Airflow, and an optional agent that runs the investigation automatically when a task fails.

This post covers how it works, what broke while I built it, and how to run it yourself on an existing Airflow setup.

## The idea: give the AI tools, not opinions

The server has no LLM inside. That was a deliberate choice. It exposes tools over the **Model Context Protocol (MCP)**, and whatever client you connect (Claude Code, Claude Desktop, or your own agent) does the reasoning.

This has a few nice consequences:

- The server is cheap, fast and deterministic. No API key needed to run it.
- You can swap the AI without touching the server.
- Every tool is read-only, so the worst an AI can do is read logs.

Here is the whole system:

```
Your Airflow  <--- read-only REST --->  Pipeline Doctor (MCP server)  <--- tools --->  Claude Code / Desktop
      |                                              ^
      | task fails                                   |
      v                                              |
 on_failure_callback ---------------------------------+
      |
      +--> evidence mode: calls the server, writes a notification file (no LLM)
      +--> agent mode:    pings the Gemini agent, which investigates via the same tools
```

## The tools

The server exposes eleven tools. The first one does most of the work.

- `investigate_run`: a one-call evidence bundle for a failed run: task states, the failing task's log, run history, the task graph, upstream and downstream DAGs, and regex failure-class hints (for example, `UndefinedColumn` suggests schema drift).
- `list_pipelines` and `search_pipelines`: find DAGs by name, tag, owner, description, or even task id.
- `get_lineage`: the task graph inside a DAG, plus cross-DAG dependencies, with a Mermaid diagram. Dependencies come from Airflow Datasets and from `ExternalTaskSensor` / `TriggerDagRunOperator` IDs found in the DAG source.
- `list_failed_runs`, `get_run_history`, `get_task_instances`, `get_task_log`, `get_dag_tasks`: drill-down tools.
- `get_table_schema` and `get_table_stats`: optional, read-only warehouse checks (columns, row count, freshness) for Postgres.

There is also an MCP *prompt*, `investigate_failure`, which is the investigation playbook: read the failing log, check history, check what changed, check the data, check upstream, then report root causes with evidence, a specific fix, and a retry plan (is the task idempotent, and which task is the earliest safe one to clear).

The server speaks both **stdio** (for local clients) and **streamable HTTP** at `/mcp`, with optional bearer-token auth.

## Three ways to use it

**1. Ask your AI.** Connect the server to your client, then ask: *"Investigate the latest failure of `orders_etl`."* The client calls the tools and writes the diagnosis.

**2. Automatic evidence (no LLM).** A drop-in Airflow failure callback calls `investigate_run` and appends a plain-text summary to a notification file: failed task, likely class, log tail, recent runs, and which downstream DAGs are blocked.

**3. Automatic diagnosis (Gemini agent).** The same callback pings a small agent service. The agent connects to the MCP server, lets Gemini call the tools in a loop, and appends a written diagnosis to another file.

You can run any one of these, or both automatic modes together.

## What broke while building it

The interesting part of a project like this is rarely the happy path. Four things bit me, and they are worth knowing about if you build something similar.

**The callback fires before Airflow records the failure.** My first real test produced this notification: `Failed tasks: none found`. The failure callback runs inside the failing task's process, before the task's final state is committed, so the API still reported the task as `running`. The fix was to pass the failing task's id from the callback to the server and always fetch that task's log, whatever state Airflow shows.

**A stale Docker image.** The agent never answered. Airflow reported `RemoteDisconnected`, and, in a nice twist, that traceback then showed up in the *next* notification's log tail. The container was running a three-hour-old image from an earlier design, listening on port 8000 while Airflow called 8001. `docker compose up` does not rebuild images that already exist. Use `--build` whenever the code changes.

**A hint that was too eager.** My regex for upstream outages matched a bare `503` and flagged a healthy-looking log. I tightened it to real HTTP 502/503/504 messages. Hints are leads for the AI, not verdicts, but sloppy leads waste its attention.

**Free-tier quotas are real.** The Gemini free tier allows roughly 20 requests per day *per model*, and one investigation uses 3 to 6. Two models were exhausted quickly; others had room. Per-minute limits are different from daily ones: Gemini's error includes a retry delay, so the agent now waits as told for per-minute limits, and only switches models when the daily quota is gone. If every model is exhausted it says so plainly instead of failing mysteriously.

## Run it yourself, step by step

These steps assume you already have Airflow 2.x running in Docker with the REST API enabled. I tested against Airflow 2.5.1 (Celery executor).

### Before you start

You need:

- Docker and Docker Compose
- A running Airflow 2.x with **basic auth enabled for the API**. In the official docker-compose file this is `AIRFLOW__API__AUTH_BACKENDS: 'airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session'`
- An Airflow username and password
- Optional, for the Gemini agent: a free API key from Google AI Studio (aistudio.google.com/apikey)
- Optional: an MCP client such as Claude Code

Airflow 3 is not supported. The tools use the Airflow 2 REST API (`/api/v1`).

### Step 1: Get the project

```bash
git clone <your-repo-url> pipeline-doctor
cd pipeline-doctor
```

The layout:

```
pipeline-doctor/
├── Dockerfile                    # MCP server image
├── docker-compose.yml            # MCP server (+ agent under a profile)
├── .env.example                  # all settings
├── doctor/
│   ├── server.py                 # MCP server: stdio + HTTP, auth, playbook prompt
│   ├── tools.py                  # the eleven tools
│   └── config.py
├── agent/
│   ├── agent.py                  # Gemini agent (webhook + queue)
│   └── Dockerfile
└── integrations/
    └── airflow_callback.py       # drop-in Airflow failure callback
```

### Step 2: Configure

```bash
cp .env.example .env
```

Open `.env` and set:

```
AIRFLOW_URL=http://host.docker.internal:8080
AIRFLOW_USER=your_user
AIRFLOW_PASSWORD=your_password

# Only for the Gemini agent:
GEMINI_API_KEY=your_key
GEMINI_MODEL=gemini-3-flash-preview,gemini-flash-latest,gemini-3.5-flash-lite
```

Notes:

- Inside Docker, `localhost` means the container itself. Use `host.docker.internal` to reach an Airflow on your own machine. On Linux, add `extra_hosts: ["host.docker.internal:host-gateway"]` to the services that need it.
- `GEMINI_MODEL` is a comma-separated list in order of preference. When one model's daily quota runs out, the agent moves to the next and records which model wrote each report. Model names change quickly, so check what your key can use.
- Optionally set `MCP_AUTH_TOKEN` to require `Authorization: Bearer <token>` on the HTTP endpoint. Do this before exposing the port beyond localhost, because the tools return task logs.

### Step 3: Start the MCP server

```bash
docker compose up -d --build
docker compose logs -f
```

The server is now at `http://localhost:8000/mcp`, bound to localhost only. Quick check:

```bash
curl -s -X POST localhost:8000/mcp \
  -H 'Content-Type: application/json' \
  -H 'Accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"t","version":"1"}}}'
```

You should get a JSON-RPC reply naming `pipeline-doctor`.

### Step 4: Connect an MCP client and try discovery

With Claude Code:

```bash
claude mcp add --transport http pipeline-doctor http://localhost:8000/mcp
```

Run `/mcp` to confirm it is connected, then ask:

- *"List my pipelines."* This also confirms your Airflow credentials work.
- *"Search pipelines for csv."*
- *"Show lineage for `<one of your dag ids>`."*

If you see `Cannot reach Airflow`, check `AIRFLOW_URL`. `Airflow API 401` means wrong credentials or API basic auth not enabled.

For stdio instead of HTTP (for example in Claude Desktop), run the server through Docker:

```bash
claude mcp add pipeline-doctor -- docker run -i --rm --env-file /abs/path/.env pipeline-doctor --transport stdio
```

(Build the image first with `docker build -t pipeline-doctor .`.)

### Step 5: Create a deliberate failure

Put this in your Airflow DAGs folder as `pd_test_fail.py`:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def boom():
    print("GET https://api.example.com/orders -> 401 Unauthorized")
    raise PermissionError("401 Unauthorized: token_expired")

with DAG("pd_test_fail", start_date=datetime(2026, 1, 1), schedule=None, catchup=False) as dag:
    PythonOperator(task_id="call_api", python_callable=boom)
```

Wait about 30 seconds for Airflow to load it, unpause it in the UI, and trigger it. It fails in a couple of seconds.

### Step 6: Investigate manually

Ask your client: *"Investigate the latest failure of `pd_test_fail`."*

A good answer classifies it as `credentials`, quotes the 401 log line, and says `call_api` is safe to retry after the token is renewed.

### Step 7: Automatic evidence (no LLM)

1. Copy the callback into your DAGs folder:

   ```bash
   cp integrations/airflow_callback.py /path/to/airflow/dags/
   ```

2. Use it in the test DAG:

   ```python
   from airflow_callback import notify_pipeline_doctor

   with DAG("pd_test_fail", ..., default_args={"on_failure_callback": notify_pipeline_doctor}) as dag:
       ...
   ```

3. Add settings to the `environment:` block under `x-airflow-common` in your Airflow `docker-compose.yaml`:

   ```yaml
   DOCTOR_MODE: evidence
   DOCTOR_MCP_URL: http://host.docker.internal:8000/mcp
   NOTIFY_FILE: /opt/airflow/logs/pipeline_doctor_notifications.log
   ```

   Then recreate the containers: `docker compose up -d` in the Airflow folder.

4. Trigger `pd_test_fail` again and watch the file (the `logs` folder is already mounted in the official compose file):

   ```bash
   tail -f /path/to/airflow/logs/pipeline_doctor_notifications.log
   ```

Within a few seconds you should see something like:

```
==== 2026-10-11T10:42:44Z | pd_test_fail | evidence ====
DAG pd_test_fail FAILED (run manual__2026-10-11T10:42:41.839488+00:00)
Failed tasks: call_api
Likely class: credentials (matched '401')
Log tail for call_api:
    PermissionError: 401 Unauthorized: token_expired
Earlier runs (newest first): failed failed
```

The callback never raises, so a problem with alerting cannot mask or change the task failure. Note that in this mode it waits for the server (up to about 45 seconds) before the task is marked finished.

### Step 8: Automatic diagnosis with the Gemini agent

1. Make sure `GEMINI_API_KEY` and `GEMINI_MODEL` are set in `.env`, then start the agent:

   ```bash
   docker compose --profile agent up -d --build
   curl localhost:8001/health
   ```

2. Switch Airflow to `both` mode by changing and adding in the same `environment:` block, then recreate the Airflow containers:

   ```yaml
   DOCTOR_MODE: both
   DOCTOR_AGENT_URL: http://host.docker.internal:8001/investigate
   ```

3. Trigger `pd_test_fail` again and watch the agent's file:

   ```bash
   tail -f notifications/pipeline_doctor.log
   ```

After 30 to 60 seconds you get a written report: verdict, failure class, ranked root causes with the log line as evidence, the specific fix, and the retry plan. In my run Gemini needed just three tool calls to reach the right answer.

You can also run it once without Airflow in the loop:

```bash
docker compose --profile agent run --rm doctor-agent --dag pd_test_fail
```

The agent also keeps its last 20 reports at `curl localhost:8001/reports`.

### Troubleshooting

- **Nothing in the evidence file:** run `docker compose logs --tail 20 pipeline-doctor`, and look for a `pipeline-doctor` error in the failed task's log in the Airflow UI.
- **Evidence works, agent file is empty:** run `docker compose logs --tail 20 doctor-agent`. "Daily quota exhausted for all configured models" means add more models to `GEMINI_MODEL` or wait for the reset.
- **Old behavior after a code change:** rebuild with `--build`.
- **Callback not loading:** make sure `airflow_callback.py` is in the DAGs folder and give Airflow a minute to re-parse.

## Safety and limits

- Every tool is read-only. The server never clears, retries or modifies anything. It recommends, and a human acts.
- Secrets (passwords, tokens, API keys) matching common patterns are redacted from logs before they leave the server.
- Table names are validated and queried with parameterized identifiers.
- Cross-DAG lineage from source code is best effort. It finds IDs written as string literals; IDs built from variables are missed, and files that define several DAGs are marked as file-level links.
- Airflow 2.x only. Postgres only for the warehouse tools.
- Without code history, the AI cannot tie a failure to the commit that caused it. I originally had a git tool for this and removed it to keep the setup small. Adding it back is a natural extension.
- Agent mode costs a little per failure on a paid key, and the free tier runs out quickly.

I should also be upfront about what I verified. The tools and both transports were tested against a mock Airflow with known dependencies. The evidence flow ran against a real Airflow 2.5.1 and produced correct notifications, and a one-off Gemini investigation of a real failed run returned a correct diagnosis. The full automatic path from the Airflow callback through the agent to Gemini is the newest piece, so test it on a non-production DAG first.

## Where this could go

- A tool for comparing a failing run's durations and row counts against its own history
- Optional git history to correlate failures with code changes
- A "propose a fix" mode that opens a pull request for a human to review
- Support for Airflow 3 and other orchestrators, since the pattern is the same

If you build something similar, my advice is the same as what this project taught me: keep the tools boring and read-only, let the model do the reasoning, and test the unglamorous edges, like timing and stale containers, as hard as the AI parts.

---

*Pipeline Doctor is built with Python, the MCP Python SDK, and the Gemini API. Questions or improvements welcome in the comments.*
