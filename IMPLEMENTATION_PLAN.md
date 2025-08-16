# RAN Quality Evaluator – Web Implementation Plan

## 1) Scope and Objectives
- Build a web system to evaluate RAN quality at site level using CQIs, traffic, and cell-change context.
- Inputs: `site_att`, `input_date`. Outputs: 7 plots, KPI pattern evaluation (Pass/Fail/Restored), neighbors view, and PDF report.
- Leverage existing code in `quality_assurance_code/` and `cell_change_evolution/` and expose via FastAPI.

## 2) Architecture Overview
- Backend: FastAPI under `backend/app/` using SQLAlchemy, Pandas. PostgreSQL for storage.
- Frontend: Vite + React (`ui/`) consuming REST API. Charts via Recharts (or ECharts) and Leaflet for maps.
- Batch jobs: Python scripts in `quality_assurance_code/` and `cell_change_evolution/` for ingestion and cell-change processing.

## 3) Data and Environment
- DB: PostgreSQL. Schemas as created by `quality_assurance_code/create_db_*.py` and `cell_change_evolution/create_db_cell_change.py`.
- Env vars: `ROOT_DIRECTORY, POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USERNAME, POSTGRES_PASSWORD` (see `.env.sample`).
- Ensure Python 3.13 compatibility (SQLAlchemy>=2.0.36).

## 4) Backend Plan (FastAPI)
- Files: `backend/app/main.py`, `backend/app/api/v1/*.py`, `backend/app/core/settings.py`.
- CORS: allow `http://localhost:5173` by default (`settings.CORS_ORIGINS`).

### 4.1 Endpoints (MVP)
- `GET /api/health` – implemented in `backend/app/api/v1/health.py`.
- `GET /api/sites/{site_att}/ranges?input_date=YYYY-MM-DD` → compute Before/After/Last ranges and `max_date`.
  - Uses `cell_change_evolution/select_db_master_node.get_max_date()`.
- `GET /api/sites/{site_att}/cqi?from=YYYY-MM-DD&to=YYYY-MM-DD&technology=3G|4G|5G` → site CQIs and related traffic.
  - Source selectors: `cell_change_evolution/select_db_cqi_daily.py`.
- `GET /api/sites/{site_att}/traffic?from=YYYY-MM-DD&to=YYYY-MM-DD&technology=3G|4G|5G&vendor=...` → site data traffic.
  - Source selectors: `*_cell_traffic_daily` via selection utils.
- `GET /api/sites/{site_att}/traffic/voice?from=YYYY-MM-DD&to=YYYY-MM-DD&technology=3G|4G&vendor=...` → site voice traffic.
  - UMTS CS + VoLTE (LTE) aggregation.
- `GET /api/sites/{site_att}/cell-changes?group_by=network|region|province|municipality&technologies=...&vendors=...` → grouped daily cell-change stacks.
  - Uses `select_db_cell_period.py` and helpers.

### 4.2 Endpoints (Phase 2)
- `GET /api/sites/{site_att}/neighbors` → list neighbors with geo (from `master_node`).
- `GET /api/sites/{site_att}/neighbors/aggregates?from=...&to=...` → neighbor CQIs and traffic aggregates.
- `POST /api/evaluate` → body `{site_att, input_date, threshold=5%, period=7, guard=7}` → per-metric 9-type classification and overall result.
- `POST /api/report` → generate PDF (see Reporting).

### 4.3 Backend Tasks
- Implement missing selectors in API layer mapping to selection utilities.
- Add pagination for heavy endpoints; default limit/window by date.
- Input validation via Pydantic models.
- Logging and error handling.

## 5) Batch and Ingestion Plan
- One-time: run `quality_assurance_code/create_db_*.py` to build schemas.
- Load data: `quality_assurance_code/insert_db_*.py` for UMTS/LTE/NR/VoLTE/master.
- Cell periods: `cell_change_evolution/insert_db_*_cell_period.py` (≥3 consecutive days logic).
- Cell changes: `cell_change_evolution/insert_db_*_cell_change.py` to produce `*_cell_change_event`.
- Recurrence: schedule daily/weekly via cron or workflow runner.

## 6) Frontend Plan (React)
- Files: `ui/src/App.tsx`, `ui/src/api.ts`.
- Inputs: `site_att` picker, `input_date` date picker; optional threshold/period/guard.
- Views/Plots (per RFP):
  - Plot01: Site CQIs (line) with vertical range markers.
  - Plot02: Site traffic data (stacked bar) with markers.
  - Plot03: Site voice traffic (stacked bar) with markers.
  - Plot04: Geo map (site green; neighbors yellow) using Leaflet.
  - Plot05: Neighbor CQIs (line) with markers.
  - Plot06: Neighbor data traffic (stacked bar) with markers.
  - Plot07: Neighbor voice traffic (stacked bar) with markers.
- Components:
  - Filters panel (technology, vendor, date range).
  - Charts: Recharts/ECharts wrappers.
  - Map: Leaflet with markers from neighbors endpoint.
  - Evaluation summary banner (Pass/Fail/Restored).
  - Export buttons (CSV for raw data, PDF from backend).

## 7) KPI Evaluation Logic
- Implement evaluator service (Phase 2) according to `RFP_Web_System_Spec.md` §6:
  - Compute range means for relevant metrics.
  - Deltas: `(After-Before)/max(|Before|, eps)`, `(Last-After)/max(|After|, eps)`.
  - Classify into 9 types and map to Pass/Fail/Restored per metric.
  - Roll-up to Evaluation Result.
- Endpoint: `POST /api/evaluate` returns structured result for UI.

## 8) Reporting (PDF)
- Option A: WeasyPrint (HTML+CSS to PDF) – produce an HTML report from UI templates, render server-side.
- Option B: ReportLab/Matplotlib – compose PDF directly.
- Plan: Phase 2 implement WeasyPrint-based HTML-to-PDF with assets, embed all plots and summary tables.
- Endpoint: `POST /api/report` returns PDF.

## 9) DevOps and Config
- `.env.sample` at repo root and `backend/.env.sample`, `ui/.env.sample` provided.
- CORS: `settings.CORS_ORIGINS` configurable.
- Local dev commands:
  - Backend: `.venv/bin/uvicorn app.main:app --reload --port 8000 --app-dir backend`
  - Frontend: `npm run dev -- --port 5173`
- Dependencies pinned (SQLAlchemy>=2.0.36 for Python 3.13).

## 10) Milestones & Timeline
- M1 (Done): Boot backend, core endpoints (ranges, cqi, traffic, cell-changes). Wire minimal UI.
- M2 (Backend): Neighbors endpoints; refine selectors; pagination and validation.
- M3 (Frontend): Plot01–03 for site; Plot04 map; Plot05–07 neighbors; UX enhancements.
- M4 (Evaluation): Implement `/api/evaluate` logic and UI summary.
- M5 (Reporting): Implement `/api/report` PDF.
- M6 (Hardening): Error handling, tests, docs, packaging.

## 11) Risks and Mitigations
- Data consistency across loaders – mitigate with incremental loads and constraints.
- Performance on large date ranges – add date filters, indices, pagination, pre-aggregation.
- KPI mapping precision – align with ingestion script fields; add config for formula weights.
- PDF rendering complexity – choose HTML-to-PDF with tested templates.

## 12) Acceptance Criteria
- Endpoint contract matches §7.1 of the spec.
- UI renders 7 plots with correct range markers and tooltips.
- Evaluation Result correct on sample scenarios (unit tests for evaluator).
- PDF report compiles all plots and tables for a site/date.
- Documentation: README for backend/frontend, ops runbook, and API schema.
