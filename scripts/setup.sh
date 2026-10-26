#!/usr/bin/env bash
# GCS -> Pub/Sub -> subscriber -> BigQuery, with a dead-letter topic.
# Usage: PROJECT_ID=my-project ./setup.sh        (add WITH_PUSH=1 for the Cloud Run push variant)
set -euo pipefail

PROJECT_ID="${PROJECT_ID:?set PROJECT_ID}"
REGION="${REGION:-europe-west9}"          # Cloud Run / Pub/Sub region hint
BQ_LOCATION="${BQ_LOCATION:-EU}"          # dataset + bucket must share a location
BUCKET="${PROJECT_ID}-landing"
TOPIC="file-events"
DLQ_TOPIC="file-events-dlq"
SUB="file-events-sub"                     # pull subscription
DLQ_SUB="file-events-dlq-sub"             # lets you inspect dead letters
DATASET="raw"
TABLE="orders"
SUBSCRIBER_SA="ingest-subscriber"
SA_EMAIL="${SUBSCRIBER_SA}@${PROJECT_ID}.iam.gserviceaccount.com"

gcloud config set project "$PROJECT_ID"
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')
PUBSUB_AGENT="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"

echo "== 1. Enable APIs"
gcloud services enable pubsub.googleapis.com storage.googleapis.com \
  bigquery.googleapis.com monitoring.googleapis.com

echo "== 2. Bucket (the data source)"
gcloud storage buckets create "gs://${BUCKET}" \
  --location="$BQ_LOCATION" --uniform-bucket-level-access

echo "== 3. Topics"
gcloud pubsub topics create "$TOPIC"
gcloud pubsub topics create "$DLQ_TOPIC"

echo "== 4. Let Cloud Storage publish to the topic (the usual silent failure if missing)"
GCS_AGENT=$(gcloud storage service-agent --project="$PROJECT_ID")
gcloud pubsub topics add-iam-policy-binding "$TOPIC" \
  --member="serviceAccount:${GCS_AGENT}" --role="roles/pubsub.publisher"

echo "== 5. Bucket notification: new object -> message on the topic"
gcloud storage buckets notifications create "gs://${BUCKET}" \
  --topic="$TOPIC" --event-types=OBJECT_FINALIZE \
  --object-prefix=incoming/ --payload-format=json

echo "== 6. Pull subscription with retries and dead-lettering"
gcloud pubsub subscriptions create "$SUB" --topic="$TOPIC" \
  --ack-deadline=60 \
  --message-retention-duration=7d --retain-acked-messages \
  --expiration-period=never \
  --min-retry-delay=10s --max-retry-delay=600s \
  --dead-letter-topic="$DLQ_TOPIC" --max-delivery-attempts=5
gcloud pubsub subscriptions create "$DLQ_SUB" --topic="$DLQ_TOPIC" --expiration-period=never

echo "== 7. Dead-lettering needs the Pub/Sub agent to forward messages"
gcloud pubsub topics add-iam-policy-binding "$DLQ_TOPIC" \
  --member="serviceAccount:${PUBSUB_AGENT}" --role="roles/pubsub.publisher"
gcloud pubsub subscriptions add-iam-policy-binding "$SUB" \
  --member="serviceAccount:${PUBSUB_AGENT}" --role="roles/pubsub.subscriber"

echo "== 8. BigQuery target"
bq --location="$BQ_LOCATION" mk --dataset "${PROJECT_ID}:${DATASET}"
bq mk --table "${PROJECT_ID}:${DATASET}.${TABLE}" \
  order_id:STRING,customer_id:STRING,amount:FLOAT,created_at:TIMESTAMP

echo "== 9. Subscriber identity, least privilege"
gcloud iam service-accounts create "$SUBSCRIBER_SA" --display-name="Ingest subscriber"
gcloud pubsub subscriptions add-iam-policy-binding "$SUB" \
  --member="serviceAccount:${SA_EMAIL}" --role="roles/pubsub.subscriber"
gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
  --member="serviceAccount:${SA_EMAIL}" --role="roles/storage.objectViewer"
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${SA_EMAIL}" --role="roles/bigquery.jobUser"
bq add-iam-policy-binding \
  --member="serviceAccount:${SA_EMAIL}" --role="roles/bigquery.dataEditor" \
  "${PROJECT_ID}:${DATASET}.${TABLE}"

# Optional: push variant, Pub/Sub POSTs to Cloud Run (same script the CD pipeline uses)
if [[ "${WITH_PUSH:-0}" == "1" ]]; then
  echo "== 10. Cloud Run push endpoint"
  PROJECT_ID="$PROJECT_ID" REGION="$REGION" BQ_LOCATION="$BQ_LOCATION" DATASET="$DATASET" TABLE="$TABLE" \
    "$(dirname "$0")/deploy_push.sh"
fi

echo "Done. Test: gcloud storage cp sample-data/orders.csv gs://${BUCKET}/incoming/"
