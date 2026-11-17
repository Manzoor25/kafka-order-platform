#!/usr/bin/env bash
set -euo pipefail

TS="$(date +%s)"
CORRUPT_KEY="ord-corrupt-${TS}"
POISON_ORDER_ID="ord-poison-${TS}"
HEALTHY_1="ord-healthy-a-${TS}"
HEALTHY_2="ord-healthy-b-${TS}"

echo "======================================================================"
echo " Failure Demo 3: Poison Message -> Non-Blocking Retry/DLT -> CLI Replay"
echo " Corrupt Key   : ${CORRUPT_KEY}"
echo " Poison Order  : ${POISON_ORDER_ID} (shippingPostalCode=POISON-TRANSIENT)"
echo " Healthy Orders: ${HEALTHY_1}, ${HEALTHY_2}"
echo "======================================================================"

echo "[1/5] Injecting corrupt non-Avro bytes onto inventory.reserved.v1 (${CORRUPT_KEY})..."
printf '%s:%s\n' "${CORRUPT_KEY}" "CORRUPT_NON_AVRO_WIRE_PAYLOAD_${TS}" | \
  docker compose exec -T kafka-1 /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server kafka-1:19092 \
    --topic inventory.reserved.v1 \
    --property "parse.key=true" \
    --property "key.separator=:"

echo "[2/5] Submitting poison order (${POISON_ORDER_ID}) followed immediately by healthy orders..."
curl -fsS -X POST "http://localhost:8080/api/orders" \
  -H "Content-Type: application/json" \
  -d "{
    \"orderId\": \"${POISON_ORDER_ID}\",
    \"customerId\": \"cust-poison\",
    \"customerEmail\": \"poison@example.com\",
    \"sku\": \"SKU-MOUSE\",
    \"quantity\": 1,
    \"totalAmountCents\": 7900,
    \"currency\": \"USD\",
    \"paymentMethodToken\": \"tok_visa_ok\",
    \"shippingPostalCode\": \"POISON-TRANSIENT\"
  }" >/dev/null

for hid in "${HEALTHY_1}" "${HEALTHY_2}"; do
  curl -fsS -X POST "http://localhost:8080/api/orders" \
    -H "Content-Type: application/json" \
    -d "{
      \"orderId\": \"${hid}\",
      \"customerId\": \"cust-healthy\",
      \"customerEmail\": \"healthy@example.com\",
      \"sku\": \"SKU-MOUSE\",
      \"quantity\": 1,
      \"totalAmountCents\": 7900,
      \"currency\": \"USD\",
      \"paymentMethodToken\": \"tok_visa_ok\",
      \"shippingPostalCode\": \"98101\"
    }" >/dev/null
done

echo "[3/5] Verifying partition keeps moving (healthy orders complete without head-of-line blocking)..."
for hid in "${HEALTHY_1}" "${HEALTHY_2}"; do
  H_STATUS=""
  for _ in $(seq 1 40); do
    H_STATUS="$(curl -fsS "http://localhost:8080/api/orders/${hid}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
    if [[ "${H_STATUS}" == "COMPLETED" ]]; then
      break
    fi
    sleep 0.5
  done
  echo "  -> Healthy order ${hid} status=${H_STATUS}"
  if [[ "${H_STATUS}" != "COMPLETED" ]]; then
    echo "[FAIL] Partition blocked! Healthy order ${hid} stuck in ${H_STATUS}" >&2
    exit 1
  fi
done

echo "[4/5] Waiting for ${POISON_ORDER_ID} to traverse .retry-0 -> .retry-1 -> .retry-2 -> inventory.reserved.v1.dlt..."
INSPECT_OUT=""
for _ in $(seq 1 30); do
  INSPECT_OUT="$(docker compose --profile tools run --rm dlq-cli \
    inspect --topic inventory.reserved.v1.dlt 2>/dev/null || true)"
  if [[ "${INSPECT_OUT}" == *"${POISON_ORDER_ID}"* && "${INSPECT_OUT}" == *"${CORRUPT_KEY}"* ]]; then
    break
  fi
  sleep 1
done

echo "  --- dlq-cli inspect output ---"
echo "${INSPECT_OUT}"
echo "  ------------------------------"

if [[ "${INSPECT_OUT}" != *"${POISON_ORDER_ID}"* ]]; then
  echo "[FAIL] Expected ${POISON_ORDER_ID} in inventory.reserved.v1.dlt" >&2
  exit 1
fi

echo "[5/5] Replaying dead-lettered ${POISON_ORDER_ID} via dlq-cli replay and verifying saga completion..."
REPLAY_OUT="$(docker compose --profile tools run --rm dlq-cli \
  replay --topic inventory.reserved.v1.dlt --key "${POISON_ORDER_ID}")"
echo "${REPLAY_OUT}"

REPLAYED_STATUS=""
for _ in $(seq 1 45); do
  REPLAYED_STATUS="$(curl -fsS "http://localhost:8080/api/orders/${POISON_ORDER_ID}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
  if [[ "${REPLAYED_STATUS}" == "COMPLETED" ]]; then
    break
  fi
  sleep 0.5
done

echo "  -> Replayed order ${POISON_ORDER_ID} final status: ${REPLAYED_STATUS}"
if [[ "${REPLAYED_STATUS}" != "COMPLETED" ]]; then
  echo "[FAIL] Expected replayed order ${POISON_ORDER_ID} to finish COMPLETED, got ${REPLAYED_STATUS}" >&2
  exit 1
fi

echo ""
echo "[PASS] Failure Demo 3 succeeded: partition stayed live, poison landed in DLT, and dlq-cli replay completed the order!"
