#!/usr/bin/env bash
set -euo pipefail

TS="$(date +%s)"
DECLINED_ORDER="ord-declined-${TS}"
SHIPFAIL_ORDER="ord-shipfail-${TS}"

echo "======================================================================"
echo " Failure Demo 4: Payment Declined & Multi-Hop Saga Compensation"
echo "======================================================================"

echo "[Part A] Submitting order ${DECLINED_ORDER} with declined card token (tok_decline_card)..."
curl -fsS -X POST "http://localhost:8080/api/orders" \
  -H "Content-Type: application/json" \
  -d "{
    \"orderId\": \"${DECLINED_ORDER}\",
    \"customerId\": \"cust-decline\",
    \"customerEmail\": \"decline@example.com\",
    \"sku\": \"SKU-MONITOR-4K\",
    \"quantity\": 1,
    \"totalAmountCents\": 59900,
    \"currency\": \"USD\",
    \"paymentMethodToken\": \"tok_decline_insufficient_funds\",
    \"shippingPostalCode\": \"98101\"
  }" >/dev/null

D_STATUS=""
D_JSON=""
for _ in $(seq 1 40); do
  D_JSON="$(curl -fsS "http://localhost:8080/api/orders/${DECLINED_ORDER}")"
  D_STATUS="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])' <<<"${D_JSON}")"
  if [[ "${D_STATUS}" == "CANCELLED" ]]; then
    break
  fi
  sleep 0.5
done

python3 - <<<"${D_JSON}" <<'PY'
import json, sys
o = json.load(sys.stdin)
print(f"  -> Order {o['orderId']} final status={o['status']}, failureStage={o['failureStage']}, reason='{o['failureReason']}'")
for h in o["history"]:
    print(f"       {h['previousStatus']} -> {h['newStatus']} via {h['triggerEvent']}")
assert o["status"] == "CANCELLED", f"Expected CANCELLED, got {o['status']}"
assert o["failureStage"] == "PAYMENT", f"Expected PAYMENT, got {o['failureStage']}"
PY

echo ""
echo "[Part B] Submitting order ${SHIPFAIL_ORDER} with unserviceable shipping zone (FAIL-SHIP-ZONE)"
echo "         to verify full upstream compensation (ShipmentFailed -> InventoryReleased -> PaymentRefunded -> OrderCancelled)..."
curl -fsS -X POST "http://localhost:8080/api/orders" \
  -H "Content-Type: application/json" \
  -d "{
    \"orderId\": \"${SHIPFAIL_ORDER}\",
    \"customerId\": \"cust-shipfail\",
    \"customerEmail\": \"shipfail@example.com\",
    \"sku\": \"SKU-MECH-KB\",
    \"quantity\": 1,
    \"totalAmountCents\": 14900,
    \"currency\": \"USD\",
    \"paymentMethodToken\": \"tok_visa_ok\",
    \"shippingPostalCode\": \"FAIL-SHIP-ZONE\"
  }" >/dev/null

S_STATUS=""
S_JSON=""
for _ in $(seq 1 50); do
  S_JSON="$(curl -fsS "http://localhost:8080/api/orders/${SHIPFAIL_ORDER}")"
  S_STATUS="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])' <<<"${S_JSON}")"
  if [[ "${S_STATUS}" == "CANCELLED" ]]; then
    break
  fi
  sleep 0.5
done

python3 - <<<"${S_JSON}" <<'PY'
import json, sys
o = json.load(sys.stdin)
print(f"  -> Order {o['orderId']} final status={o['status']}, failureStage={o['failureStage']}, reason='{o['failureReason']}'")
for h in o["history"]:
    print(f"       {h['previousStatus']} -> {h['newStatus']} via {h['triggerEvent']}")
assert o["status"] == "CANCELLED", f"Expected CANCELLED, got {o['status']}"
assert o["failureStage"] == "SHIPPING_FAILED", f"Expected SHIPPING_FAILED, got {o['failureStage']}"
PY

PAY_AUDIT="$(curl -fsS "http://localhost:8082/internal/diagnostics/payments/${SHIPFAIL_ORDER}")"
python3 - <<<"${PAY_AUDIT}" <<'PY'
import json, sys
doc = json.load(sys.stdin)
ops = [op["operationType"] for op in doc["gatewayOperations"]]
print(f"  -> Payment ledger status={doc['payment']['status']}, gateway operations={ops}")
assert doc["payment"]["status"] == "REFUNDED"
assert "CHARGE" in ops and "REFUND" in ops
PY

echo ""
echo "[PASS] Failure Demo 4 succeeded: both declined payment and downstream shipping failure compensated cleanly to CANCELLED!"
