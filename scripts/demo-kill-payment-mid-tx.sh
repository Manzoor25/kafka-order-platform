#!/usr/bin/env bash
set -euo pipefail

ORDER_ID="ord-midtx-$(date +%s)"

echo "======================================================================"
echo " Failure Demo 2: Kill payment-service Mid-Kafka-Transaction"
echo " Scenario : Charge external card gateway, flush uncommitted PaymentAuthorized"
echo "            into open Kafka TX, SIGKILL payment-service before commit,"
echo "            restart it, and prove customer is charged EXACTLY ONCE."
echo " Order ID : ${ORDER_ID}"
echo "======================================================================"

cleanup() {
  docker compose up -d payment-service >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[1/5] Submitting order ${ORDER_ID} with fault token tok_hold_mid_tx..."
curl -fsS -X POST "http://localhost:8080/api/orders" \
  -H "Content-Type: application/json" \
  -d "{
    \"orderId\": \"${ORDER_ID}\",
    \"customerId\": \"cust-midtx\",
    \"customerEmail\": \"midtx@example.com\",
    \"sku\": \"SKU-HEADSET\",
    \"quantity\": 1,
    \"totalAmountCents\": 19900,
    \"currency\": \"USD\",
    \"paymentMethodToken\": \"tok_hold_mid_tx\",
    \"shippingPostalCode\": \"98101\"
  }" >/dev/null

echo "[2/5] Waiting for payment-service to execute external card charge and hold open Kafka TX..."
DETECTED="false"
for _ in $(seq 1 60); do
  if docker compose logs --since 60s payment-service 2>/dev/null | grep -q "MID_TX_HOLD_READY orderId=${ORDER_ID}"; then
    DETECTED="true"
    break
  fi
  sleep 0.3
done

if [[ "${DETECTED}" != "true" ]]; then
  echo "[FAIL] Did not observe MID_TX_HOLD_READY for ${ORDER_ID} in payment-service logs" >&2
  docker compose logs --tail=50 payment-service >&2 || true
  exit 1
fi

MID_STATUS="$(curl -fsS "http://localhost:8080/api/orders/${ORDER_ID}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
echo "  -> Order status while Kafka TX is uncommitted (read_committed isolation): ${MID_STATUS}"

echo "[3/5] >>> [FAULT INJECTED] Sending SIGKILL to payment-service mid-transaction!"
docker compose kill -s SIGKILL payment-service

echo "[4/5] Restarting payment-service (fences stale producer epoch & redelivers uncommitted OrderCreated)..."
docker compose up -d payment-service
for _ in $(seq 1 60); do
  if curl -fsS "http://localhost:8082/actuator/health" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

FINAL_STATUS=""
for _ in $(seq 1 60); do
  FINAL_STATUS="$(curl -fsS "http://localhost:8080/api/orders/${ORDER_ID}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
  if [[ "${FINAL_STATUS}" == "COMPLETED" ]]; then
    break
  fi
  sleep 1
done

if [[ "${FINAL_STATUS}" != "COMPLETED" ]]; then
  echo "[FAIL] Expected order ${ORDER_ID} to finish COMPLETED after recovery, got ${FINAL_STATUS}" >&2
  exit 1
fi

echo "[5/5] Auditing external card gateway operations for ${ORDER_ID}:"
AUDIT_JSON="$(curl -fsS "http://localhost:8082/internal/diagnostics/payments/${ORDER_ID}")"

python3 - <<<"${AUDIT_JSON}" <<'PY'
import json, sys
audit = json.load(sys.stdin)
ops = [op for op in audit["gatewayOperations"] if op["operationType"] == "CHARGE"]
assert len(ops) == 1, f"Expected 1 CHARGE record, found {len(ops)}: {ops}"
charge = ops[0]
print(f"  -> Idempotency Key         : {charge['idempotencyKey']}")
print(f"  -> Gateway Reference ID    : {charge['gatewayReferenceId']}")
print(f"  -> Actual Card Charges     : {charge['executionCount']} (MUST be 1)")
print(f"  -> Deduplicated Redelivery : {charge['deduplicatedRetryCount']} (>= 1 proves redelivery hit idempotency key)")
assert charge["executionCount"] == 1, f"Customer double-charged! executionCount={charge['executionCount']}"
assert charge["deduplicatedRetryCount"] >= 1, f"Expected deduplicatedRetryCount >= 1 after SIGKILL recovery, got {charge['deduplicatedRetryCount']}"
PY

echo ""
echo "[PASS] Failure Demo 2 succeeded: payment-service crashed mid-TX, recovered cleanly, and charged customer EXACTLY ONCE!"
