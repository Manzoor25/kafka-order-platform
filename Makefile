SHELL := /bin/bash

.PHONY: help up demo test test-unit down logs ps \
        demo-kill-broker demo-kill-payment demo-poison-dlt demo-payment-declined \
        demo-scale-kip848 demo-schema-evolution

help: ## Show available targets
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-24s\033[0m %s\n", $$1, $$2}'

up: ## Build and start the 3-broker Kafka 4.4.0 KRaft cluster, 5 DBs, Apicurio, Jaeger, Kafka UI, and all 5 services
	docker compose up -d --build
	docker compose build dlq-cli
	bash scripts/wait-for-stack.sh

demo: ## Run end-to-end happy-path order saga and verify 5-service OpenTelemetry trace in Jaeger
	bash scripts/demo-happy-path.sh

test: ## Run full Maven suite: unit tests, Avro BACKWARD contract gate, per-service Testcontainers ITs, and 5-service E2E ITs
	./mvnw -B -ntp verify

test-unit: ## Run fast unit, contract, and zero-inter-service-HTTP architecture tests (no Docker required)
	./mvnw -B -ntp test

down: ## Stop all containers and remove volumes
	docker compose --profile tools down -v --remove-orphans

logs: ## Follow microservice logs
	docker compose logs -f order-service payment-service inventory-service shipping-service notification-service

ps: ## Show container status
	docker compose ps

demo-kill-broker: ## Failure Demo 1: Kill a Kafka broker under live order load — 0 acknowledged messages lost
	bash scripts/demo-kill-broker-under-load.sh

demo-kill-payment: ## Failure Demo 2: SIGKILL payment-service mid-Kafka-transaction — customer charged once
	bash scripts/demo-kill-payment-mid-tx.sh

demo-poison-dlt: ## Failure Demo 3: Send poison messages -> DLT while partition keeps moving, then inspect & replay via dlq-cli
	bash scripts/demo-poison-message-dlt.sh

demo-payment-declined: ## Failure Demo 4: Payment declined & shipping failure -> choreographed saga compensates to CANCELLED
	bash scripts/demo-payment-declined.sh

demo-scale-kip848: ## Scale inventory-service from 1 -> 3 instances under KIP-848 (group.protocol=consumer) and inspect partitions
	bash scripts/demo-scale-kip848.sh

demo-schema-evolution: ## Demo compatible Avro evolution (accepted) vs breaking Avro change (rejected with non-zero CI exit code)
	bash scripts/demo-schema-evolution.sh
