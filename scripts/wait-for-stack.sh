#!/usr/bin/env bash
set -euo pipefail

echo "======================================================================"
echo " Waiting for Kafka Order Platform stack to report healthy..."
echo "======================================================================"

wait_http() {
  local name="$1"
  local url="$2"
  local max_attempts="${3:-60}"
  local attempt=1
  while (( attempt <= max_attempts )); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      echo "  [READY] ${name} (${url})"
      return 0
    fi
    sleep 2
    ((attempt++))
  done
  echo "  [ERROR] Timed out waiting for ${name} at ${url}" >&2
  docker compose ps >&2 || true
  return 1
}

wait_container_healthy() {
  local service="$1"
  local max_attempts="${2:-60}"
  local attempt=1
  while (( attempt <= max_attempts )); do
    local ids
    ids="$(docker compose ps -q "$service" 2>/dev/null || true)"
    if [[ -n "$ids" ]]; then
      local all_ok=true
      for cid in $ids; do
        local health
        health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$cid" 2>/dev/null || echo "starting")"
        if [[ "$health" != "healthy" ]]; then
          all_ok=false
          break
        fi
      done
      if [[ "$all_ok" == "true" ]]; then
        echo "  [READY] ${service} container(s) healthy"
        return 0
      fi
    fi
    sleep 2
    ((attempt++))
  done
  echo "  [ERROR] Timed out waiting for ${service} container health" >&2
  docker compose ps >&2 || true
  docker compose logs --tail=50 "$service" >&2 || true
  return 1
}

wait_container_healthy "kafka-1" 60
wait_container_healthy "kafka-2" 60
wait_container_healthy "kafka-3" 60
wait_http "Apicurio Schema Registry" "http://localhost:8081/apis/registry/v3/system/info" 60
wait_http "Jaeger Tracing UI" "http://localhost:16686/" 45
wait_http "order-service" "http://localhost:8080/actuator/health" 75
wait_http "payment-service" "http://localhost:8082/actuator/health" 75
wait_container_healthy "inventory-service" 75
wait_container_healthy "shipping-service" 75
wait_http "notification-service" "http://localhost:8085/actuator/health" 75

# Ensure global BACKWARD compatibility rule is active in Apicurio Registry
curl -fsS -X POST "http://localhost:8081/apis/registry/v3/admin/rules" \
  -H "Content-Type: application/json" \
  -d '{"ruleType":"COMPATIBILITY","config":"BACKWARD"}' >/dev/null 2>&1 || true

echo ""
echo "Verifying Topics-as-Code (partitions=6, replication-factor=3, min.insync.replicas=2):"
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 \
  --describe --topic order.created.v1 | head -n 3

echo ""
echo "Stack is UP and healthy:"
echo "  - Order Ingress API : http://localhost:8080/api/orders"
echo "  - Apicurio Registry : http://localhost:8081/apis/registry/v3"
echo "  - Kafbat Kafka UI   : http://localhost:8086"
echo "  - Jaeger Tracing UI : http://localhost:16686"
