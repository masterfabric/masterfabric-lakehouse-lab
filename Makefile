# MasterFabric Lakehouse Lab. Run `make help` for the list of targets.
SHELL := /bin/bash
COMPOSE := docker compose --env-file .env -f docker/compose.yaml
SPARK   := docker exec -w /lab mf-lake-spark
SUBMIT  := $(SPARK) spark-submit --driver-memory $${SPARK_DRIVER_MEMORY:-3g}
SCALE   ?= 1
DAYS    ?= 7
PY      ?= .venv/bin/python

.PHONY: help env up down ps logs generate bronze silver gold quality demos scala pipeline \
        test test-unit test-spark bench trino-sql notebook screenshots assets clean-data reset

help: ## Show targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-12s\033[0m %s\n",$$1,$$2}'

env: ## Create .env from .env.example (never overwrites)
	@test -f .env || cp .env.example .env

up: env ## Start object storage, catalog, Spark and Trino
	mkdir -p data/landing
	$(COMPOSE) up -d --build
	@echo "S3 console http://127.0.0.1:19001  Trino http://127.0.0.1:18080  Jupyter http://127.0.0.1:18888  Spark history http://127.0.0.1:18090"

down: ## Stop the stack (keeps volumes)
	$(COMPOSE) down

ps: ## Show service status
	$(COMPOSE) ps -a

logs: ## Tail service logs
	$(COMPOSE) logs -f --tail=50

generate: ## Generate synthetic raw feeds (SCALE=1 DAYS=7)
	$(SPARK) python -m generator.telco_gen --out /landing --scale $(SCALE) --days $(DAYS)

bronze: ## Land raw files into bronze Iceberg tables (idempotent)
	$(SUBMIT) lakehouse/bronze.py

silver: ## Clean, deduplicate, conform and MERGE into silver
	$(SUBMIT) lakehouse/silver.py

gold: ## Build business marts
	$(SUBMIT) lakehouse/gold.py

quality: ## Run data quality rules and reconcile against the generator manifest
	$(SUBMIT) lakehouse/quality.py

demos: ## Iceberg feature demos: partition/schema evolution, time travel, MERGE, compaction
	$(SUBMIT) lakehouse/iceberg_demos.py

scala: ## Run the Scala job (cell congestion ranking) through spark-shell
	$(SPARK) bash -c 'spark-shell --driver-memory 2g -I jobs/scala/CellCongestionRanking.scala < /dev/null'

pipeline: ## Full run: generate -> bronze -> silver -> gold -> quality, with timings
	$(SPARK) python scripts/run_pipeline.py --scale $(SCALE) --days $(DAYS)

test: test-unit test-spark ## All tests

test-unit: ## Generator and helper tests (host Python)
	$(PY) -m pytest -q tests/unit

test-spark: ## Spark transformation tests (inside the Spark container, local mode)
	docker exec -w /lab -e PYTHONPATH=/lab:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip mf-lake-spark python -m pytest -q -p no:cacheprovider tests/spark

bench: ## Measure the pipeline at several scales and write docs/results
	docker exec -w /lab -e LAB_HOST="$$(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $$(( $$(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1073741824 )) GB RAM" mf-lake-spark python scripts/benchmark.py

trino-sql: ## Run the showcase Trino queries and save their output
	$(PY) scripts/trino_queries.py

notebook: ## Build and execute notebooks/lakehouse_tour.ipynb inside the Spark container
	$(SPARK) python scripts/build_notebook.py
	docker exec -w /lab -e PYTHONPATH=/lab:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip mf-lake-spark jupyter nbconvert --to notebook --execute --inplace --ExecutePreprocessor.timeout=600 notebooks/lakehouse_tour.ipynb

screenshots: ## Capture UI screenshots with Playwright (stack must be up)
	$(PY) scripts/screenshots.py

assets: ## Render charts, diagrams and the hero image from real results
	$(PY) scripts/make_charts.py && $(PY) scripts/make_hero.py

clean-data: ## Delete generated landing files (not the lake)
	rm -rf data/landing/*

reset: ## Stop the stack AND delete its volumes (all lake data)
	$(COMPOSE) down -v
