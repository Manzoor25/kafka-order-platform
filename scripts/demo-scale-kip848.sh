#!/usr/bin/env bash
set -euo pipefail

GROUP_ID="inventory-service-group"

echo "======================================================================"
echo " Demo: KIP-848 Consumer Scaling (1 -> 3 Instances across 6 Partitions)"
echo " Group : ${GROUP_ID} (group.protocol=consumer)"
echo "======================================================================"

cleanup() {
  docker compose up -d --scale inventory-service=1 >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "[1/4] Ensuring inventory-service is running at 1 instance..."
docker compose up -d --scale inventory-service=1 >/dev/null
sleep 4

echo ""
echo "--- BEFORE SCALING (1 Consumer Instance -> 6 Partitions per Topic) ---"
BEFORE_OUT="$(docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:19092 \
  --describe --group "${GROUP_ID}" --members --verbose)"
echo "${BEFORE_OUT}"

echo ""
echo "[2/4] Scaling inventory-service from 1 -> 3 instances under KIP-848..."
docker compose up -d --scale inventory-service=3

for _ in $(seq 1 45); do
  IDS="$(docker compose ps -q inventory-service)"
  COUNT="$(echo "${IDS}" | wc -w | tr -d ' ')"
  ALL_HEALTHY="true"
  if [[ "${COUNT}" != "3" ]]; then
    ALL_HEALTHY="false"
  else
    for cid in ${IDS}; do
      st="$(docker inspect --format '{{.State.Health.Status}}' "${cid}" 2>/dev/null || echo "starting")"
      if [[ "${st}" != "healthy" ]]; then
        ALL_HEALTHY="false"
        break
      fi
    done
  fi
  if [[ "${ALL_HEALTHY}" == "true" ]]; then
    break
  fi
  sleep 2
done

# Wait briefly for KIP-848 server-side uniform assignor convergence (2, 2, 2)
AFTER_OUT=""
for _ in $(seq 1 20); do
  AFTER_OUT="$(docker compose exec -T kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server kafka-1:19092 \
    --describe --group "${GROUP_ID}" --members --verbose)"
  MEMBER_LINES="$(grep -c 'payment.authorized.v1' <<<"${AFTER_OUT}" || true)"
  if (( MEMBER_LINES == 3 )); then
    break
  fi
  sleep 1
done

echo ""
echo "--- AFTER SCALING (3 Consumer Instances -> 2 + 2 + 2 Partitions per Topic) ---"
echo "${AFTER_OUT}"

if (( MEMBER_LINES != 3 )); then
  echo "[FAIL] Expected 3 KIP-848 group members holding payment.authorized.v1 partitions, saw ${MEMBER_LINES}" >&2
  exit 1
fi

echo ""
echo "[3/4] Submitting 6 concurrent orders across partitions while scaled to 3 instances..."
TS="$(date +%s)"
SCALE_ORDERS=()
for i in $(seq 1 6); do
  OID="ord-scale-${TS}-${i}"
  SCALE_ORDERS+=("${OID}")
  curl -fsS -X POST "http://localhost:8080/api/orders" \
    -H "Content-Type: application/json" \
    -d "{
      \"orderId\": \"${OID}\",
      \"customerId\": \"cust-scale-${i}\",
      \"customerEmail\": \"scale${i}@example.com\",
      \"sku\": \"SKU-HEADSET\",
      \"quantity\": 1,
      \"totalAmountCents\": 19900,
      \"currency\": \"USD\",
      \"paymentMethodToken\": \"tok_visa_scale\",
      \"shippingPostalCode\": \"98101\"
    }" >/dev/null
done

for oid in "${SCALE_ORDERS[@]}"; do
  for _ in $(seq 1 40); do
    st="$(curl -fsS "http://localhost:8080/api/orders/${oid}" | python3 -c 'import json,sys; print(json.load(sys.stdin)["status"])')"
    [[ "${st}" == "COMPLETED" ]] && break
    sleep 0.5
  done
  if [[ "${st}" != "COMPLETED" ]]; then
    echo "[FAIL] Order ${oid} did not complete while scaled to 3 instances (status=${st})" >&2
    exit 1
  fi
done
echo "  -> All 6/6 orders completed across the 3 KIP-848 consumer instances!"

echo "[4/4] Scaling inventory-service back to 1 instance..."
docker compose up -d --scale inventory-service=1 >/dev/null

echo ""
echo "[PASS] KIP-848 consumer group scaled 1 -> 3 (6 partitions -> 2, 2, 2) with zero lost orders!"
