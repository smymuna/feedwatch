.PHONY: install check lint typecheck test integration up down logs demo usdc dashboard-dev

PY := .venv/bin

install:            ## create .venv and install everything for development
	python3.12 -m venv .venv
	$(PY)/pip install -e ".[dev]"
	cd dashboard && npm ci

check: lint typecheck test   ## everything CI runs without Docker

lint:
	$(PY)/ruff check src tests
	$(PY)/ruff format --check src tests

typecheck:
	$(PY)/mypy

test:
	$(PY)/pytest -q
	cd dashboard && npm test

integration:        ## needs: docker compose up -d kafka db
	$(PY)/pytest -m integration -v

up:                 ## the live pipeline: http://localhost:8000
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f ingest detect

demo:               ## a fresh stack fed by the recorded session with every fault injected
	docker compose down -v
	docker compose up -d --build kafka db topics metrics detect store api
	@echo "Dashboard: http://localhost:8000 (replaying for 5 minutes)"
	docker compose run --rm --no-deps ingest replay samples/session --scenario scenarios/drill.json

usdc:               ## replay the real March 2023 USDC depeg through the detector
	$(PY)/feedwatch backtest samples/history/kraken_USDCUSD_2023-03-10_2023-03-13.csv.gz --symbol USDC-USD

dashboard-dev:      ## dashboard with hot reload against a running API
	cd dashboard && npm run dev
