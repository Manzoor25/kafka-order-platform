#!/usr/bin/env bash
set -euo pipefail

BASELINE_DIR="platform-events/src/test/resources/avro-baseline"
CURRENT_DIR="platform-events/src/main/avro"
COMPATIBLE_DIR="platform-events/src/test/resources/avro-evolution/compatible"
BREAKING_DIR="platform-events/src/test/resources/avro-evolution/breaking-required-field"

echo "======================================================================"
echo " Demo: Avro Schema Evolution & BACKWARD Compatibility CI Enforcement"
echo "======================================================================"

echo "[1/3] Validating production schemas (${CURRENT_DIR}) against baseline..."
./mvnw -B -ntp -q -pl platform-events test-compile exec:java \
  -Dexec.mainClass="io.orderplatform.events.SchemaBackwardCompatibilityValidator" \
  -Dexec.args="${BASELINE_DIR} ${CURRENT_DIR}"

echo ""
echo "[2/3] Demo 1 — BACKWARD-Compatible Evolution (optional field with null default):"
echo "      Candidate: ${COMPATIBLE_DIR}/OrderCreated.avsc"
./mvnw -B -ntp -q -pl platform-events exec:java \
  -Dexec.mainClass="io.orderplatform.events.SchemaBackwardCompatibilityValidator" \
  -Dexec.args="${BASELINE_DIR} ${COMPATIBLE_DIR}"

echo ""
echo "[3/3] Demo 2 — Breaking Schema Change (new required field without default — CI MUST reject):"
echo "      Candidate: ${BREAKING_DIR}/OrderCreated.avsc"
set +e
BREAKING_OUT="$(./mvnw -B -ntp -q -pl platform-events exec:java \
  -Dexec.mainClass="io.orderplatform.events.SchemaBackwardCompatibilityValidator" \
  -Dexec.args="${BASELINE_DIR} ${BREAKING_DIR}" 2>&1)"
BREAKING_RC=$?
set -e

echo "${BREAKING_OUT}"
if (( BREAKING_RC == 0 )); then
  echo "[FAIL] Breaking schema change was NOT rejected!" >&2
  exit 1
fi
echo "  -> CI gate exited with non-zero code (${BREAKING_RC}) as expected."

if curl -fsS "http://localhost:8081/apis/registry/v3/system/info" >/dev/null 2>&1; then
  echo ""
  echo "[Live Registry Check] Verifying Apicurio Registry v3 BACKWARD rule enforcement on localhost:8081..."
  ARTIFACT_ID="demo.order.created.$(date +%s)-value"
  V1_RAW="$(cat "${BASELINE_DIR}/OrderCreated.avsc")"
  V2_OK_RAW="$(cat "${COMPATIBLE_DIR}/OrderCreated.avsc")"
  V2_BAD_RAW="$(cat "${BREAKING_DIR}/OrderCreated.avsc")"

  python3 - "${ARTIFACT_ID}" "${V1_RAW}" "${V2_OK_RAW}" "${V2_BAD_RAW}" <<'PY'
import json, sys, urllib.request, urllib.error

artifact_id, v1, v2_ok, v2_bad = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
base = "http://localhost:8081/apis/registry/v3/groups/order-platform"

def post_json(url, payload):
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    return urllib.request.urlopen(req, timeout=10)

# 1. Register v1
post_json(f"{base}/artifacts", {
    "artifactId": artifact_id,
    "artifactType": "AVRO",
    "firstVersion": {"version": "1", "content": {"content": v1, "contentType": "application/json"}},
})
# Ensure artifact-level BACKWARD rule
post_json(f"{base}/artifacts/{artifact_id}/rules", {"ruleType": "COMPATIBILITY", "config": "BACKWARD"})
print(f"  -> Registered v1 + BACKWARD rule on Apicurio artifact '{artifact_id}'")

# 2. Compatible v2 -> 200 OK
post_json(f"{base}/artifacts/{artifact_id}/versions", {
    "version": "2",
    "content": {"content": v2_ok, "contentType": "application/json"},
})
print("  -> Apicurio ACCEPTED compatible v2 evolution (HTTP 200)")

# 3. Breaking v3 -> HTTP 409 RuleViolationException
try:
    post_json(f"{base}/artifacts/{artifact_id}/versions", {
        "version": "3",
        "content": {"content": v2_bad, "contentType": "application/json"},
    })
    raise SystemExit("[FAIL] Apicurio unexpectedly accepted breaking v3!")
except urllib.error.HTTPError as err:
    assert err.code in (400, 409, 422), f"Unexpected HTTP status {err.code}"
    print(f"  -> Apicurio REJECTED breaking v3 evolution with HTTP {err.code} RuleViolation!")
PY
fi

echo ""
echo "[PASS] Schema evolution demo verified: compatible v2 accepted, breaking change rejected!"
