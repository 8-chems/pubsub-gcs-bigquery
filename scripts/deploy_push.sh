#!/usr/bin/env bash
# Idempotent: deploys the Cloud Run push endpoint and wires a push subscription to it.
# Safe to re-run (CD runs it on every deploy).
#   IMAGE=...   use a prebuilt image (CD does this); otherwise builds from ../app with Cloud Build
set -euo pipefail

PROJECT_ID="${PROJECT_ID:?set PROJECT_ID}"
REGION="${REGION:-europe-west9}"
BQ_LOCATION="${BQ_LOCATION:-EU}"
DATASET="${DATASET:-raw}"
TABLE="${TABLE:-orders}"
TOPIC="file-events"
DLQ_TOPIC="file-events-dlq"
SERVICE="ingest-push"
PUSH_SUB="file-events-push-sub"
SA_EMAIL="ingest-subscriber@${PROJECT_ID}.iam.gserviceaccount.com"
PUSH_SA_EMAIL="pubsub-push-invoker@${PROJECT_ID}.iam.gserviceaccount.com"

PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')
PUBSUB_AGENT="service-${PROJECT_NUMBER}@gcp-sa-pubsub.iam.gserviceaccount.com"

gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com \
  --project="$PROJECT_ID"

echo "== Push identity (the account Pub/Sub uses to call Cloud Run)"
gcloud iam service-accounts describe "$PUSH_SA_EMAIL" --project="$PROJECT_ID" >/dev/null 2>&1 ||
  gcloud iam service-accounts create pubsub-push-invoker \
    --display-name="Pub/Sub push identity" --project="$PROJECT_ID"
# May be a no-op on newer projects; lets Pub/Sub mint OIDC tokens for that identity.
gcloud iam service-accounts add-iam-policy-binding "$PUSH_SA_EMAIL" \
  --member="serviceAccount:${PUBSUB_AGENT}" --role="roles/iam.serviceAccountTokenCreator" \
  --project="$PROJECT_ID" >/dev/null

echo "== Deploy Cloud Run service"
if [[ -n "${IMAGE:-}" ]]; then
  SOURCE_ARGS=(--image="$IMAGE")
else
  SOURCE_ARGS=(--source="$(cd "$(dirname "$0")/../app" && pwd)")
fi
gcloud run deploy "$SERVICE" "${SOURCE_ARGS[@]}" \
  --project="$PROJECT_ID" --region="$REGION" \
  --no-allow-unauthenticated --service-account="$SA_EMAIL" \
  --set-env-vars="GCP_PROJECT=${PROJECT_ID},BQ_DATASET=${DATASET},BQ_TABLE=${TABLE},BQ_LOCATION=${BQ_LOCATION}"

gcloud run services add-iam-policy-binding "$SERVICE" \
  --project="$PROJECT_ID" --region="$REGION" \
  --member="serviceAccount:${PUSH_SA_EMAIL}" --role="roles/run.invoker" >/dev/null

URL=$(gcloud run services describe "$SERVICE" --project="$PROJECT_ID" --region="$REGION" \
  --format='value(status.url)')

echo "== Push subscription -> $URL"
if gcloud pubsub subscriptions describe "$PUSH_SUB" --project="$PROJECT_ID" >/dev/null 2>&1; then
  gcloud pubsub subscriptions update "$PUSH_SUB" --project="$PROJECT_ID" \
    --push-endpoint="$URL" --push-auth-service-account="$PUSH_SA_EMAIL"
else
  gcloud pubsub subscriptions create "$PUSH_SUB" --project="$PROJECT_ID" --topic="$TOPIC" \
    --push-endpoint="$URL" --push-auth-service-account="$PUSH_SA_EMAIL" \
    --ack-deadline=120 --expiration-period=never \
    --dead-letter-topic="$DLQ_TOPIC" --max-delivery-attempts=5
fi
gcloud pubsub subscriptions add-iam-policy-binding "$PUSH_SUB" --project="$PROJECT_ID" \
  --member="serviceAccount:${PUBSUB_AGENT}" --role="roles/pubsub.subscriber" >/dev/null

echo "Deployed. Service URL: $URL"
