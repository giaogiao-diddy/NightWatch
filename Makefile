SHELL := /bin/bash
.DEFAULT_GOAL := help

.PHONY: help install install-backend install-frontend dev-api dev-worker dev-mcp dev-web dev-stack test test-unit test-integration lint build-web up-prod down-prod restart-prod logs-prod

help:
	@echo "NightWatch Developer Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make install              Install backend + frontend dependencies"
	@echo "  make install-backend      pip install -r requirements.txt"
	@echo "  make install-frontend     npm install"
	@echo ""
	@echo "Local Development:"
	@echo "  make dev-api              Start FastAPI server"
	@echo "  make dev-worker           Start background worker"
	@echo "  make dev-mcp              Start FastMCP server"
	@echo "  make dev-web              Start Next.js frontend"
	@echo "  make dev-stack            Start API + Worker + MCP + Web together"
	@echo ""
	@echo "Quality:"
	@echo "  make test                 Run all tests"
	@echo "  make test-unit            Run unit tests"
	@echo "  make test-integration     Run integration tests"
	@echo "  make lint                 Run Next.js lint"
	@echo "  make build-web            Build frontend"
	@echo ""
	@echo "Production Compose:"
	@echo "  make up-prod              docker compose up -d --build"
	@echo "  make down-prod            docker compose down"
	@echo "  make restart-prod         docker compose restart"
	@echo "  make logs-prod            Tail compose logs"

install: install-backend install-frontend

install-backend:
	pip install -r requirements.txt

install-frontend:
	npm install

dev-api:
	uvicorn nightwatch.main:app --reload --port 8000

dev-worker:
	python -m nightwatch.worker

dev-mcp:
	python -m nightwatch.mcp.server

dev-web:
	npm run dev

dev-stack:
	python scripts/run_demo_stack.py

test:
	pytest tests/ -v

test-unit:
	pytest tests/unit/ -v

test-integration:
	pytest tests/integration/ -v

lint:
	npm run lint

build-web:
	npm run build

up-prod:
	docker compose -f docker-compose.prod.yml up -d --build

down-prod:
	docker compose -f docker-compose.prod.yml down

restart-prod:
	docker compose -f docker-compose.prod.yml restart

logs-prod:
	docker compose -f docker-compose.prod.yml logs -f --tail=200
