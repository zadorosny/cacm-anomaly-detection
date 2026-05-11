# CA/CM Anomaly Detection — task runner.
# Usage: `make help` lists every available target.

SHELL        := /bin/bash
PYTHON       ?= python3
POETRY       ?= poetry
RUN          := $(POETRY) run

# Pipeline parameters (override on the command line, e.g. `make data DIAS=90 AGENCIAS=100`)
DIAS         ?= 30
AGENCIAS     ?= 50
ASSOCIADOS   ?= 10000
OPERADORES   ?= 200
TX_POR_DIA   ?= 5000
SEED         ?= 42
EPOCHS       ?= 40

.DEFAULT_GOAL := help

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------
.PHONY: help
help:  ## Show this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n\nTargets:\n"} \
	/^[a-zA-Z_-]+:.*##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------
.PHONY: install
install:  ## Install runtime + dev deps via Poetry
	$(POETRY) install --with dev
	$(RUN) pre-commit install

.PHONY: install-pip
install-pip:  ## Alternative: install via pip (no Poetry required)
	$(PYTHON) -m pip install -r requirements-dev.txt

# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------
.PHONY: data
data:  ## Generate synthetic transactions + dimensions (bronze parquet)
	$(RUN) python scripts/gerar_dados.py \
		--dias $(DIAS) --agencias $(AGENCIAS) --associados $(ASSOCIADOS) \
		--operadores $(OPERADORES) --tx-por-dia $(TX_POR_DIA) --seed $(SEED)

.PHONY: train
train:  ## Train Isolation Forest + Autoencoder (writes data/models/)
	$(RUN) python scripts/treinar_modelos.py --epochs $(EPOCHS)

.PHONY: pipeline
pipeline:  ## Run bronze -> silver -> gold + alert scoring
	$(RUN) python scripts/rodar_pipeline.py

.PHONY: all
all: install data train pipeline  ## Full end-to-end: install -> data -> train -> pipeline

.PHONY: clean-data
clean-data:  ## Remove generated parquets, checkpoints and models (keeps source)
	rm -rf data/bronze data/silver data/gold data/checkpoints data/models data/landing
	rm -rf mlruns spark-warehouse metastore_db derby.log

# ---------------------------------------------------------------------------
# Quality gates (mirror of GitHub Actions CI)
# ---------------------------------------------------------------------------
.PHONY: lint
lint:  ## Ruff lint
	$(RUN) ruff check src tests

.PHONY: format
format:  ## Auto-format with ruff format
	$(RUN) ruff format src tests

.PHONY: format-check
format-check:  ## Verify formatting without writing
	$(RUN) ruff format --check src tests

.PHONY: typecheck
typecheck:  ## Mypy static type-check
	$(RUN) mypy src

.PHONY: test
test:  ## Run unit tests
	$(RUN) pytest -m "not integration"

.PHONY: test-integration
test-integration:  ## Run integration tests (requires Spark/Java 17)
	$(RUN) pytest -m integration

.PHONY: check
check: lint format-check typecheck test  ## Run every CI gate locally

# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------
.PHONY: mlflow
mlflow:  ## Launch the MLflow UI on http://localhost:5000
	$(RUN) mlflow ui --port 5000

.PHONY: docs
docs:  ## Serve MkDocs documentation on http://localhost:8000
	$(RUN) mkdocs serve
