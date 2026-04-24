# =============================================================================
# mini-lakehouse — Makefile
# Alle Werte kommen aus .env — keine hardcodierten Werte in diesem File.
# =============================================================================

include .env
export

COMPOSE         := docker compose
COMPOSE_FILE    := docker-compose.yml
DBT_DIR         := dbt
SCRIPTS_DIR     := scripts

# Farben
GREEN  := \033[0;32m
YELLOW := \033[0;33m
RESET  := \033[0m

# Plattform-Detection für Browser-Öffnung
UNAME := $(shell uname -s)
ifeq ($(UNAME),Darwin)
  OPEN := open
else
  OPEN := xdg-open
endif

.PHONY: help up down clean status logs logs-spark logs-trino seed demo health \
        dbt-run dbt-test dbt-docs restart pull reset-demo1

help: ## Zeigt alle verfügbaren Targets mit Beschreibung
	@echo ""
	@echo "  mini-lakehouse — verfügbare Targets:"
	@echo ""
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z_-]+:.*##/ { \
		printf "  $(GREEN)%-16s$(RESET) %s\n", $$1, $$2 \
	}' $(MAKEFILE_LIST)
	@echo ""

up: ## Startet alle Services im Hintergrund (docker compose up -d)
	$(COMPOSE) up -d

down: ## Stoppt alle Services
	$(COMPOSE) down

clean: ## Stoppt Services und löscht Volumes und verwaiste Container
	$(COMPOSE) down -v --remove-orphans

status: ## Zeigt den Status aller Container
	$(COMPOSE) ps

logs: ## Folgt den Logs aller Services
	$(COMPOSE) logs -f

logs-spark: ## Folgt den Logs von spark-master und spark-worker
	$(COMPOSE) logs -f spark-master spark-worker

logs-trino: ## Folgt den Logs von Trino
	$(COMPOSE) logs -f trino

seed: ## Lädt Beispieldaten (scripts/seed-data.sh)
	bash $(SCRIPTS_DIR)/seed-data.sh

demo: seed ## Lädt Beispieldaten und öffnet Jupyter im Browser
	$(OPEN) "http://localhost:$(JUPYTER_PORT)?token=$(JUPYTER_TOKEN)"

health: ## Prüft den Gesundheitszustand aller Services (scripts/healthcheck.sh)
	bash $(SCRIPTS_DIR)/healthcheck.sh

dbt-run: ## Führt dbt run im dbt-Verzeichnis aus
	cd $(DBT_DIR) && uv run dbt run

dbt-test: ## Führt dbt test im dbt-Verzeichnis aus
	cd $(DBT_DIR) && uv run dbt test

dbt-docs: ## Generiert und startet die dbt-Dokumentation
	cd $(DBT_DIR) && uv run dbt docs generate && uv run dbt docs serve

restart: down up ## Startet alle Services neu (down + up)

pull: ## Lädt alle Docker-Images vorab (für Offline-Demo)
	$(COMPOSE) pull

reset-demo1: ## Setzt Demo 1 (Fondspreise) auf Startzustand zurück
	bash $(SCRIPTS_DIR)/reset-demo1.sh
