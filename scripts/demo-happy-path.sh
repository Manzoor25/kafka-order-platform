#!/usr/bin/env bash
set -euo pipefail

ORDER_ID="ord-happy-$(date +%s)"
echo "======================================================================"
echo " Demo: End-to-End Choreographed Order Saga + 5-Service W3C Tracing"
echo " Order ID: ${ORDER_ID}"
echo "======================================================================"

CREATE_RESP="$(curl -fsS -X POST "http://localhost:8080/api/orders" \
  -H "Content-Type: application/json" \
  -d "{
    \"orderId\": \"${ORDER_ID}\",
    \"customerId\": \"cust-demo-01\",
    \"customerEmail\": \"buyer@example.com\",
    \"sku\": \"SKU-LAPTOP-PRO\",
    \"quantity\": 1,
    \"totalAmountCents\": 249900,
    \"currency\": \"USD\",
    \"paymentMethodToken\": \"tok_visa_4242\",
    \"shippingPostalCode\": \"98101\",
    \"couponCode\": \"LAUNCH10\"
  }")"

TRACE_ID="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["traceId"])' <<<"${CREATE_RESP}")"
echo "[1/4] Order accepted via Transactional Outbox -> status=CREATED, traceId=${TRACE_ID}"

echo "[2/4] Waiting for choreographed Kafka saga (order -> payment -> inventory -> shipping -> order)..."
STATUS=""
ORDER_JSON=""
for _ in $(seq 1 60); do
  ORDER_JSON="$(curl -fsS "http://localhost:8080/api/orders/${ORDER_ID}")"
  STATUS="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])' <<<"${ORDER_JSON}")"
  if [[ "${STATUS}" == "COMPLETED" ]]; then
    break
  fi
  sleep 0.5
done

if [[ "${STATUS}" != "COMPLETED" ]]; then
  echo "[FAIL] Expected status=COMPLETED, got status=${STATUS}" >&2
  echo "${ORDER_JSON}" >&2
  exit 1
fi

ORDER_JSON_PAYLOAD="${ORDER_JSON}" python3 - <<'PY'
import json, os
o = json.loads(os.environ["ORDER_JSON_PAYLOAD"])
print(f"  -> Final Status   : {o['status']}")
print(f"  -> Payment ID     : {o['paymentId']}")
print(f"  -> Reservation ID : {o['reservationId']}")
print(f"  -> Shipment ID    : {o['shipmentId']} (tracking={o['trackingNumber']})")
print("  -> Saga History   :")
for h in o["history"]:
    print(f"       {h['previousStatus']} -> {h['newStatus']} via {h['triggerEvent']} ({h['details']})")
PY

echo "[3/4] Verifying customer notifications & W3C traceId propagation in notification-service..."
NOTIF_COUNT=0
NOTIF_JSON="[]"
for _ in $(seq 1 30); do
  NOTIF_JSON="$(curl -fsS "http://localhost:8085/internal/diagnostics/notifications/${ORDER_ID}")"
  NOTIF_COUNT="$(python3 -c 'import json,sys; print(len(json.load(sys.stdin)))' <<<"${NOTIF_JSON}")"
  if (( NOTIF_COUNT >= 2 )); then
    break
  fi
  sleep 0.5
done

NOTIF_JSON_PAYLOAD="${NOTIF_JSON}" EXPECTED_TRACE_ID="${TRACE_ID}" python3 - <<'PY'
import json, os
expected_trace = os.environ["EXPECTED_TRACE_ID"]
items = json.loads(os.environ["NOTIF_JSON_PAYLOAD"])
types = {n["notificationType"] for n in items}
assert "SHIPMENT_DISPATCHED" in types and "ORDER_CONFIRMED" in types, f"Missing expected notifications: {types}"
for n in items:
    print(f"  -> Notification [{n['notificationType']}] traceId={n['traceId']} subject='{n['subject']}'")
    assert n["traceId"] == expected_trace, f"Trace mismatch: {n['traceId']} != {expected_trace}"
PY

echo "[4/4] Verifying single correlated distributed trace across all 5 services in Jaeger..."
SERVICES_CSV=""
for _ in $(seq 1 30); do
  JAEGER_RESP="$(curl -fsS "http://localhost:16686/api/traces/${TRACE_ID}" 2>/dev/null || echo '{"data":[]}')"
  SERVICES_CSV="$(python3 -c '
import json, sys
doc = json.load(sys.stdin)
data = doc.get("data") or []
if not data:
    print("")
else:
    procs = data[0].get("processes", {})
    svcs = sorted({p.get("serviceName", "") for p in procs.values() if p.get("serviceName")})
    print(",".join(svcs))
' <<<"${JAEGER_RESP}")"
  if [[ "${SERVICES_CSV}" == *"inventory-service"* && \
        "${SERVICES_CSV}" == *"notification-service"* && \
        "${SERVICES_CSV}" == *"order-service"* && \
        "${SERVICES_CSV}" == *"payment-service"* && \
        "${SERVICES_CSV}" == *"shipping-service"* ]]; then
    break
  fi
  sleep 1
done

echo "  -> Jaeger services on trace ${TRACE_ID}: ${SERVICES_CSV}"
for svc in order-service payment-service inventory-service shipping-service notification-service; do
  if [[ "${SERVICES_CSV}" != *"${svc}"* ]]; then
    echo "[FAIL] Missing ${svc} span in Jaeger trace ${TRACE_ID}" >&2
    exit 1
  fi
done

echo ""
echo "[PASS] Happy-path saga COMPLETED and traced end-to-end across all 5 microservices!"
echo "       Inspect in Jaeger UI: http://localhost:16686/trace/${TRACE_ID}"
