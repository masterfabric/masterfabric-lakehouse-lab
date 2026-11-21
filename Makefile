# MasterFabric Lakehouse Lab. Run `make help` for the list of targets.
SHELL := /bin/bash
COMPOSE := docker compose --env-file .env -f docker/compose.yaml
SPARK   := docker exec -w /lab mf-lake-spark
SUBMIT  := $(SPARK) spark-submit --driver-memory $${SPARK_DRIVER_MEMORY:-3g}
SCALE   ?= 1
DAYS    ?= 7
PY      ?= .venv/bin/python

.PHONY: help env up down ps logs generate archive bronze silver gold quality demos scala pipeline \
        test test-unit test-spark bench lineage-up lineage-down lineage wap lineage-export trino-sql notebook screenshots assets clean-data reset

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

archive: ## Copy landing files to the s3://landing raw archive (immutable copy of what arrived)
	docker run --rm --network mf-lakehouse_default --env-file .env -v $(CURDIR)/data/landing:/landing:ro --entrypoint sh amazon/aws-cli:latest \
	  -c 'AWS_ACCESS_KEY_ID=$$LAKE_S3_ACCESS_KEY AWS_SECRET_ACCESS_KEY=$$LAKE_S3_SECRET_KEY AWS_DEFAULT_REGION=$$LAKE_S3_REGION aws --endpoint-url http://s3:9000 s3 sync --only-show-errors /landing s3://landing/raw/'
	@echo "landing archived to s3://landing/raw/"

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
	$(SPARK) python scripts/build_notebook.py --strip-stderr

screenshots: ## Capture UI screenshots with Playwright (stack must be up)
	$(PY) scripts/screenshots.py

assets: ## Render charts, diagrams and the hero image from real results
	cd scripts && ../$(PY) make_diagrams.py && ../$(PY) make_charts.py && ../$(PY) make_hero.py

# --- Lineage, data quality and write-audit-publish ---------------------------------------------
LINEAGE := --conf spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener \
           --conf spark.openlineage.transport.type=http \
           --conf spark.openlineage.transport.url=http://marquez:5000 \
           --conf spark.openlineage.namespace=mf-lakehouse
LRESULTS := -e LAKE_RESULTS=/lab/.out/lineage-run

lineage-up: env ## Start Marquez (OpenLineage backend + UI on 127.0.0.1:13100)
	$(COMPOSE) --profile lineage up -d marquez-db marquez marquez-web

lineage-down: ## Stop Marquez (keeps its volume)
	$(COMPOSE) --profile lineage stop marquez-web marquez marquez-db

lineage: ## Purge, regenerate and run bronze -> silver -> gold -> quality with OpenLineage on
	$(SUBMIT) lakehouse/admin.py purge
	rm -rf data/landing/*
	$(SPARK) python -m generator.telco_gen --out /landing --scale $(SCALE) --days $(DAYS)
	for j in bronze silver gold quality; do docker exec -w /lab $(LRESULTS) mf-lake-spark spark-submit --driver-memory 3g $(LINEAGE) lakehouse/$$j.py || exit 1; done

wap: ## Write-audit-publish on Iceberg branches with a Great Expectations gate (lineage on)
	docker exec -w /lab mf-lake-spark spark-submit --driver-memory 3g $(LINEAGE) lakehouse/wap.py

lineage-export: ## Save the lineage graph from Marquez to docs/results/lineage.json
	$(PY) scripts/lineage_export.py

clean-data: ## Delete generated landing files (not the lake)
	rm -rf data/landing/*

reset: ## Stop the stack AND delete its volumes (all lake data)
	$(COMPOSE) down -v
