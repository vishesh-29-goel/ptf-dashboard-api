# PTF Dashboard API

FastAPI backend for the PTF L1 Sanctions Screening Dashboard.

Reads live from the agent-managed Postgres database and exposes REST endpoints
for the dashboard frontend.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/api/stats` | Overall summary stats + accuracy |
| GET | `/api/groups` | All scenario groups with counts |
| GET | `/api/alerts` | Paginated alerts (filter by `group`, `decision`) |
| GET | `/api/alerts/{scenario_id}` | Full detail for a single alert |
| GET | `/api/accuracy` | Accuracy scorecard per group |
| GET | `/api/kb-audit` | Recent Intelligence Layer KB audit entries |
| GET | `/api/runs` | Recent screening runs |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | Postgres connection string (agent-managed DB) |
| `ZAMP_API_KEY` | Zamp API key for Conversations API (Intelligence Layer resume path). Falls back to embedded key if not set. |
| `ZAMP_BASE_URL` | Zamp API base URL (default: `https://api-us.zamp.ai`) |

## Deploy on Railway

1. Connect this repo to Railway
2. Set `DATABASE_URL` in Railway environment variables
3. Railway auto-detects `Procfile` and runs `uvicorn`

## Run locally

```bash
pip install -r requirements.txt
DATABASE_URL=<your-db-url> uvicorn main:app --reload
```

API docs available at `http://localhost:8000/docs`
