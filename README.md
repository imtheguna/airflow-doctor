# Pipeline Doctor (MCP server)

Gives an AI client read-only tools to investigate failed Airflow pipelines: logs, run history, task
graph, warehouse schema/freshness, plus a one-call evidence bundle with
failure-class hints. The server contains no LLM; your MCP client does the reasoning.

Transports: **stdio** (local) and **streamable HTTP** at `/mcp`.

## Tools
| Tool | Purpose |
|---|---|
| `investigate_run(dag_id, run_id?)` | Evidence bundle: task states, failing logs, history, task graph, hints |
| `list_pipelines(limit?, offset?, tags?)` | All DAGs with schedule, tags, owners, paused flag, last run state |
| `search_pipelines(query, tags?, include_tasks?)` | Free-text search over DAG id, description, tags, owners, file (and tasks) |
| `get_lineage(dag_id)` | Task graph + upstream/downstream DAGs (Datasets, ExternalTaskSensor, TriggerDagRunOperator) + Mermaid diagram |
| `list_failed_runs`, `get_run_history`, `get_task_instances`, `get_task_log`, `get_dag_tasks` | Airflow drill-down |
| `get_table_schema`, `get_table_stats` | Postgres schema and row count / freshness (optional) |

Prompt: `investigate_failure(dag_id, run_id?)` is the full investigation playbook.

## Configure
`cp .env.example .env`, then set `AIRFLOW_URL/USER/PASSWORD`.
Targets the Airflow 2.x REST API (`/api/v1`) with basic auth; Airflow 3 is untested.

## Run: HTTP
```
docker compose up -d --build            # http://localhost:8000/mcp (bound to localhost only)
claude mcp add --transport http pipeline-doctor http://localhost:8000/mcp
# with MCP_AUTH_TOKEN set in .env:
claude mcp add --transport http pipeline-doctor http://localhost:8000/mcp --header "Authorization: Bearer <token>"
```
Set `MCP_AUTH_TOKEN` before exposing the port beyond localhost: the tools return task logs.

## Run: stdio
Docker (no local Python needed):
```
docker build -t pipeline-doctor .
claude mcp add pipeline-doctor -- docker run -i --rm --env-file /abs/path/.env \
  pipeline-doctor --transport stdio
```
Claude Desktop (`claude_desktop_config.json`):
```json
{"mcpServers": {"pipeline-doctor": {"command": "docker", "args": [
  "run","-i","--rm","--env-file","/abs/path/.env",
  "pipeline-doctor","--transport","stdio"]}}}
```
Or locally: `pip install -r requirements.txt && python -m doctor.server --transport stdio` (needs the env vars set).

## Automatic on failure
Airflow can't push to an MCP client, so a callback does it. Copy `integrations/airflow_callback.py` into your DAGs
repo and set it as the failure callback (stdlib only):
```python
from airflow_callback import notify_pipeline_doctor
default_args = {"on_failure_callback": notify_pipeline_doctor}
```
Choose a mode with `DOCTOR_MODE` in Airflow's environment:

| Mode | What happens | Needs |
|---|---|---|
| `evidence` (default) | Callback calls the MCP tool `investigate_run` and appends logs, hints, history and downstream impact to a notification file. **No LLM.** | `DOCTOR_MCP_URL`, `DOCTOR_MCP_TOKEN`, `NOTIFY_FILE` |
| `agent` | Callback pings the agent service; Gemini investigates through the MCP tools and writes a written diagnosis to the agent's notification file. | `DOCTOR_AGENT_URL`, `DOCTOR_AGENT_TOKEN`; agent needs `GEMINI_API_KEY` |
| `both` | Both of the above. | all of the above |

Start the agent (option 3) with `docker compose --profile agent up -d` (webhook `POST :8001/investigate`; diagnoses are appended to `./notifications/pipeline_doctor.log` and served at `GET :8001/reports`).
One-off: `docker compose --profile agent run --rm doctor-agent --dag my_dag`.
The agent uses `GEMINI_MODEL`, a comma-separated list in order of preference (default `gemini-3-flash-preview,gemini-flash-latest`).
It waits out per-minute rate limits, moves to the next model when a daily quota is used up, and ends each report with `[model: ...]`.
The free tier allows about 20 requests/day per model and each investigation uses 3-6.

### Notification files
- Evidence mode: the callback appends to `NOTIFY_FILE` **on the machine/container where Airflow runs** (default `<tmpdir>/pipeline_doctor_notifications.log`).
  If Airflow runs in Docker, set `NOTIFY_FILE` to a path on a mounted volume to read it from the host.
- Agent mode: `./notifications/pipeline_doctor.log` on the host (mounted into the agent container).
- Follow it with `tail -f <file>`. Each entry starts with `==== <UTC time> | ... ====`.

## Use
In your client: "Investigate the latest failure of `my_dag`" or run the `investigate_failure` prompt.
