.PHONY: build install test clean upload deploy help

VOLUME_PATH ?= /Volumes/shared/packages/wheels
DIST_DIR    := dist
WHEEL       := $(wildcard $(DIST_DIR)/*.whl)

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

test: ## Run all tests
	python3 -m pytest tests/ -q --tb=short

test-howto: ## Run HOWTO example validation tests
	python3 -m pytest tests/test_howto_examples.py -v --tb=short

test-live: ## Run HOWTO examples against real Databricks workspace
	@echo "Usage: make test-live SOURCE=<catalog> [DEST=<catalog>] [WAREHOUSE=<id>]"
	@test -n "$(SOURCE)" || (echo "ERROR: SOURCE required. Example: make test-live SOURCE=edp_dev" && exit 1)
	./scripts/test_howto_live.sh $(SOURCE) $(or $(DEST),$(SOURCE)_howto_test) $(WAREHOUSE)

test-all: test test-howto ## Run all tests including HOWTO examples

api-dev: ## Start FastAPI backend (port 8000)
	cd $(CURDIR) && uvicorn api.main:app --reload --port 8000

ui-dev: ## Start Vite React frontend (port 3000)
	cd ui && npm run dev

web-start: ## Start both API + UI in one terminal
	./scripts/start_web.sh

docs-start: ## Start documentation site (port 3001)
	./scripts/start_docs.sh

docs-build: ## Build documentation site for production
	cd docs && npm run build

lint: ## Run linter (ruff)
	python3 -m ruff check src/ tests/

clean: ## Remove build artifacts
	rm -rf dist/ build/ *.egg-info .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

build: clean test ## Build wheel package (runs tests first)
	python3 -m build --wheel --outdir $(DIST_DIR)
	@echo ""
	@echo "Wheel built: $$(ls $(DIST_DIR)/*.whl)"

install: build ## Build and install locally
	pip install --force-reinstall --no-deps $$(ls $(DIST_DIR)/*.whl | head -1)
	@echo "Installed. Verify: clone-catalog --help"

install-dev: ## Install in editable mode for development
	pip install -e ".[dev]"

upload: build ## Build and upload wheel to Databricks Volume
	databricks fs cp $$(ls $(DIST_DIR)/*.whl | head -1) \
		dbfs:$(VOLUME_PATH)/$$(basename $$(ls $(DIST_DIR)/*.whl | head -1)) --overwrite
	@echo ""
	@echo "Uploaded to: $(VOLUME_PATH)/$$(basename $$(ls $(DIST_DIR)/*.whl | head -1))"
	@echo "In notebook: %pip install $(VOLUME_PATH)/$$(basename $$(ls $(DIST_DIR)/*.whl | head -1))"

deploy: build ## Build + install locally + upload to Databricks Volume
	pip install --force-reinstall --no-deps $$(ls $(DIST_DIR)/*.whl | head -1)
	databricks fs cp $$(ls $(DIST_DIR)/*.whl | head -1) \
		dbfs:$(VOLUME_PATH)/$$(basename $$(ls $(DIST_DIR)/*.whl | head -1)) --overwrite
	@echo ""
	@echo "Deployed to both local and Databricks Volume."

# Production build
.PHONY: build-ui prod docker docker-up

build-ui: ## Build frontend for production
	cd ui && npm run build

prod: build-ui ## Run production server (builds UI first)
	uvicorn api.main:app --host 0.0.0.0 --port 8000

docker: ## Build Docker image
	docker build -t clone-xs .

docker-up: ## Start with docker-compose
	docker-compose up --build
