#!/usr/bin/env bash
set -euo pipefail

TOTAL_ORDERS=30
KILL_AT_ORDER=10
RUN_TAG="$(date +%s)"

echo "======================================================================"
echo " Failure Demo 1: Kill a Kafka Broker Under Live Order Load"
echo " Scenario : Submit ${TOTAL_ORDERS} real orders (RF=3, min.insync.replicas=2, acks=all),"
echo "            SIGKILL broker kafka-2 mid-flight at order #${KILL_AT_ORDER},"
echo "            and prove 0 acknowledged orders/events are lost."
echo "======================================================================"

cleanup() {
  docker compose start kafka-2 >/dev/null 2>&1 || true
}
trap cleanup EXIT

ACKED_ORDERS=()

echo "[1/5] Submitting ${TOTAL_ORDERS} orders while injecting hard SIGKILL on kafka-2 at order #${KILL_AT_ORDER}..."
for i in $(seq 1 "${TOTAL_ORDERS}"); do
  OID="ord-bkill-${RUN_TAG}-$(printf '%02d' "$i")"
  HTTP_CODE="$(curl -sS -o /dev/null -w '%{http_code}' -X POST "http://localhost:8080/api/orders" \
    -H "Content-Type: application/json" \
    -d "{
      \"orderId\": \"${OID}\",
      \"customerId\": \"cust-load-${i}\",
      \"customerEmail\": \"load${i}@example.com\",
      \"sku\": \"SKU-MECH-KB\",
      \"quantity\": 1,
      \"totalAmountCents\": 14900,
      \"currency\": \"USD\",
      \"paymentMethodToken\": \"tok_visa_load\",
      \"shippingPostalCode\": \"98101\"
    }")"
  if [[ "${HTTP_CODE}" == "201" ]]; then
    ACKED_ORDERS+=("${OID}")
  else
    echo "  [WARN] Order ${OID} returned HTTP ${HTTP_CODE} (not acknowledged)"
  fi

  if (( i == KILL_AT_ORDER )); then
    echo "  >>> [FAULT INJECTED] Sending SIGKILL to broker kafka-2 after ${#ACKED_ORDERS[@]} acknowledged orders..."
    docker compose kill -s SIGKILL kafka-2
  fi
done

ACKED_COUNT="${#ACKED_ORDERS[@]}"
echo "[2/5] Total HTTP 201 acknowledged orders: ${ACKED_COUNT}/${TOTAL_ORDERS}"
if (( ACKED_COUNT < TOTAL_ORDERS )); then
  echo "[FAIL] Expected all ${TOTAL_ORDERS} orders to be accepted via transactional outbox, got ${ACKED_COUNT}" >&2
  exit 1
fi

echo "[3/5] Inspecting partition leaders & shrunk ISR while kafka-2 (node 2) is DOWN:"
docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:19092 \
  --describe --topic order.created.v1

echo "[4/5] Verifying EVERY acknowledged order (${ACKED_COUNT}/${ACKED_COUNT}) completes across all 5 services with 0 loss..."
COMPLETED_COUNT=0
for attempt in $(seq 1 90); do
  COMPLETED_COUNT=0
  for oid in "${ACKED_ORDERS[@]}"; do
    st="$(curl -fsS "http://localhost:8080/api/orders/${oid}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
    if [[ "${st}" == "COMPLETED" ]]; then
      ((COMPLETED_COUNT+=1))
    fi
  done
  if (( COMPLETED_COUNT == ACKED_COUNT )); then
    break
  fi
  sleep 1
done

echo "  -> Acknowledged orders : ${ACKED_COUNT}"
echo "  -> Completed orders    : ${COMPLETED_COUNT}"
echo "  -> Lost messages       : $(( ACKED_COUNT - COMPLETED_COUNT ))"

if (( COMPLETED_COUNT != ACKED_COUNT )); then
  echo "[FAIL] Message loss or stalled saga detected: ${COMPLETED_COUNT}/${ACKED_COUNT} completed" >&2
  exit 1
fi

echo "[5/5] Restarting kafka-2 and waiting for ISR recovery (0 under-replicated partitions)..."
docker compose start kafka-2
for _ in $(seq 1 60); do
  URP="$(docker compose exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka-1:19092 \
    --describe --under-replicated-partitions | wc -l | tr -d ' ')"
  if [[ "${URP}" == "0" ]]; then
    break
  fi
  sleep 2
done

echo "  -> Under-replicated partitions after broker recovery: ${URP}"
echo ""
echo "[PASS] Failure Demo 1 succeeded: ${COMPLETED_COUNT}/${ACKED_COUNT} acknowledged orders completed with 0 message loss!"
