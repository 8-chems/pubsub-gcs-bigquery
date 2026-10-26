#!/usr/bin/env bash
# End-to-end test against real GCP: upload a one-row CSV and wait until that row is in
# BigQuery exactly once. Needs a running subscriber (local pull or Cloud Run push).
#   PROJECT_ID=my-project ./scripts/smoke_test.sh
set -euo pipefail

PROJECT_ID="${PROJECT_ID:?set PROJECT_ID}"
BUCKET="${BUCKET:-${PROJECT_ID}-landing}"
TIMEOUT="${TIMEOUT:-240}"
RUN_ID="smoke-$(date +%s)"
FILE="$(mktemp)"

printf 'order_id,customer_id,amount,created_at\n%s,C-smoke,1.00,2026-01-01 00:00:00\n' "$RUN_ID" >"$FILE"
echo "Uploading ${RUN_ID}.csv to gs://${BUCKET}/incoming/"
gcloud storage cp "$FILE" "gs://${BUCKET}/incoming/${RUN_ID}.csv" --project="$PROJECT_ID"

count_rows() {
  bq query --project_id="$PROJECT_ID" --use_legacy_sql=false --format=csv --quiet \
    "SELECT COUNT(*) FROM \`${PROJECT_ID}.raw.orders\` WHERE order_id = '${RUN_ID}'" | tail -n1
}

deadline=$(($(date +%s) + TIMEOUT))
while (($(date +%s) < deadline)); do
  n=$(count_rows)
  if [[ "$n" == "1" ]]; then
    echo "PASS: ${RUN_ID} loaded exactly once"
    bq query --project_id="$PROJECT_ID" --use_legacy_sql=false --quiet \
      "DELETE FROM \`${PROJECT_ID}.raw.orders\` WHERE order_id = '${RUN_ID}'" >/dev/null || true
    exit 0
  elif [[ "$n" -gt 1 ]]; then
    echo "FAIL: ${RUN_ID} loaded ${n} times (idempotency broken)"
    exit 1
  fi
  echo "  waiting for the row..."
  sleep 10
done

echo "FAIL: row not in BigQuery after ${TIMEOUT}s. Check the subscriber logs and the dashboard."
exit 1
