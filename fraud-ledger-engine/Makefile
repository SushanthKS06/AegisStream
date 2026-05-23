# ============================================================================
# Makefile for Real-Time Fraud Detection & Distributed Ledger Engine
# Staff Software Engineer Toolbelt
# ============================================================================

.PHONY: help infra-up infra-down create-topics test-charge db-shell redis-shell rpk-status build-all run-ingestion run-ml run-processor run-ledger seed-db test-all clean

# Default shell
SHELL := powershell.exe

help:
	@echo "============================================================================"
	@echo "Real-Time Fraud Detection & Distributed Ledger Engine - Control Panel"
	@echo "============================================================================"
	@echo "Infrastructure Orchestration:"
	@echo "  make infra-up          - Spin up Postgres, Redis, Redpanda, and Console"
	@echo "  make infra-down        - Tear down all containers and delete volumes"
	@echo "  make create-topics     - Explicitly pre-create Kafka topics in Redpanda"
	@echo "  make rpk-status        - Print current Redpanda cluster status"
	@echo ""
	@echo "Interactive Shells:"
	@echo "  make db-shell          - Enter interactive PostgreSQL psql shell"
	@echo "  make redis-shell       - Enter interactive Redis CLI shell"
	@echo ""
	@echo "Service Control:"
	@echo "  make build-all         - Compile both Go services (Ingestion, Ledger)"
	@echo "  make run-ingestion     - Spin up ingestion-api locally"
	@echo "  make run-ml            - Start ML Inference API (FastAPI) locally"
	@echo "  make run-processor     - Start Python Stream Processor locally"
	@echo "  make run-ledger        - Start ledger-worker locally"
	@echo ""
	@echo "Utilities & Testing:"
	@echo "  make seed-db           - Wipe and seed accounts table"
	@echo "  make test-charge       - Make a test POST /charge HTTP request"
	@echo "  make clean             - Clean build binaries and temp files"
	@echo "============================================================================"

# --- Infrastructure ---

infra-up:
	docker compose up -d
	@echo "Waiting for services to become healthy..."
	docker compose ps
	@echo "Postgres: http://localhost:5432"
	@echo "Redis:    http://localhost:6379"
	@echo "Redpanda: http://localhost:9092"
	@echo "Console:  http://localhost:8080"

infra-down:
	docker compose down -v

create-topics:
	docker compose exec redpanda rpk topic create raw-transactions approved-transactions flagged-transactions -p 3

rpk-status:
	docker compose exec redpanda rpk cluster status

# --- Interactive Shells ---

db-shell:
	docker compose exec postgres psql -U postgres -d fraud_ledger

redis-shell:
	docker compose exec redis redis-cli

# --- Code Compiles & Runs ---

build-all:
	@echo "Building Ingestion API..."
	cd ingestion-api && go build -o ../bin/ingestion-api.exe main.go
	@echo "Building Ledger Worker..."
	cd ledger-worker && go build -o ../bin/ledger-worker.exe main.go

run-ingestion:
	cd ingestion-api && go run main.go

run-ml:
	cd ml-serving && uvicorn main:app --reload --port 8000

run-processor:
	cd stream-processor && python main.py

run-ledger:
	cd ledger-worker && go run main.go

# --- Utilities & Testing ---

seed-db:
	docker compose exec -T postgres psql -U postgres -d fraud_ledger -f /docker-entrypoint-initdb.d/01-init.sql

test-charge:
	curl -i -X POST http://localhost:8081/charge \
		-H "Content-Type: application/json" \
		-d "{\"txn_id\":\"txn_test_$$(Get-Date -Format 'yyyyMMddHHmmss')\",\"user_id\":\"user_alice\",\"amount\":450.50,\"device_id\":\"dev_chrome_win11\"}"

clean:
	if (Test-Path -Path bin) { Remove-Item -Recurse -Force bin }
