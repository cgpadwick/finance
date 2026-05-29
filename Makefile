# Momentum screener — common dev commands.
# Backend runs on :8077 (8000 was occupied on this machine); the Vite dev
# server proxies /api -> :8077, so the frontend uses same-origin URLs.

API_PORT ?= 8077
PY := backend/.venv/bin/python
UVICORN := backend/.venv/bin/uvicorn

.PHONY: help setup ingest validate test api web build serve dev

help:
	@echo "setup     - create venv + install backend & frontend deps"
	@echo "ingest    - backfill adjusted daily bars into DuckDB (LIMIT=600)"
	@echo "validate  - prove split adjustment is correct (AAPL/NVDA/TSLA/AMZN)"
	@echo "test      - run backend pytest suite"
	@echo "api       - run FastAPI on :$(API_PORT)"
	@echo "web       - run Vite dev server (proxies to api)"
	@echo "build     - build the React app into frontend/dist"
	@echo "serve     - build frontend, then serve SPA + API together on :$(API_PORT)"

setup:
	cd backend && uv venv && uv pip install -e ".[dev]"
	cd frontend && npm install

ingest:
	cd backend && .venv/bin/python -m finance.ingest --limit $(or $(LIMIT),600)

validate:
	cd backend && .venv/bin/python -m finance.validate

test:
	cd backend && .venv/bin/python -m pytest -q

api:
	cd backend && ../$(UVICORN) api.main:app --reload --port $(API_PORT)

web:
	cd frontend && npm run dev -- --port 5173

build:
	cd frontend && npm run build

# Production-style: one server (FastAPI) serves the built SPA + the API.
serve: build
	cd backend && ../$(UVICORN) api.main:app --port $(API_PORT)
