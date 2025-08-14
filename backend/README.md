# RAN Quality Evaluator API (FastAPI)

## Prerequisites
- Python 3.11+
- Postgres running and accessible (optionally PostGIS for neighbor features)
- Project-level `.env` at repo root (see `.env.sample`)

## Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
```

## Run (dev)
```bash
uvicorn app.main:app --reload --port ${API_PORT:-8000} --app-dir backend
```

Visit:
- Root: http://localhost:8000/
- Health: http://localhost:8000/api/health
- Docs: http://localhost:8000/docs

## Environment Variables
Uses repo-root `.env` loaded via `python-dotenv`.
- API_DEBUG, API_PORT, CORS_ORIGINS
- POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USERNAME, POSTGRES_PASSWORD
- ENABLE_NEIGHBORS, NEIGHBOR_SEARCH_RADIUS_KM

## Structure
- `app/main.py`: FastAPI app, CORS, routers
- `app/core/settings.py`: env settings
- `app/api/v1/health.py`: health endpoint

## Next
- Add DAL adapters to reuse `cell_change_evolution/select_db_*.py`
- Implement endpoints from `RFP_Web_System_Spec.md` section 7.1
