#!/usr/bin/env bash
# One-time setup so GitHub Actions can deploy WITHOUT service-account keys
# (Workload Identity Federation). Run locally as a project owner.
#   PROJECT_ID=my-project GITHUB_REPO=my-user/my-repo ./scripts/bootstrap_cicd.sh
set -euo pipefail

PROJECT_ID="${PROJECT_ID:?set PROJECT_ID}"
GITHUB_REPO="${GITHUB_REPO:?set GITHUB_REPO as owner/repo}"
REGION="${REGION:-europe-west9}"
STATE_BUCKET="${PROJECT_ID}-tfstate"
DEPLOYER="github-deployer"
DEPLOYER_EMAIL="${DEPLOYER}@${PROJECT_ID}.iam.gserviceaccount.com"
POOL="github-pool"
PROVIDER="github-provider"

gcloud config set project "$PROJECT_ID" >/dev/null
PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format='value(projectNumber)')

echo "== APIs needed before Terraform can run"
gcloud services enable iam.googleapis.com iamcredentials.googleapis.com sts.googleapis.com \
  cloudresourcemanager.googleapis.com serviceusage.googleapis.com storage.googleapis.com

echo "== Terraform state bucket (versioned)"
gcloud storage buckets describe "gs://${STATE_BUCKET}" >/dev/null 2>&1 ||
  gcloud storage buckets create "gs://${STATE_BUCKET}" --location="$REGION" --uniform-bucket-level-access
gcloud storage buckets update "gs://${STATE_BUCKET}" --versioning

echo "== Deployer service account"
gcloud iam service-accounts describe "$DEPLOYER_EMAIL" >/dev/null 2>&1 ||
  gcloud iam service-accounts create "$DEPLOYER" --display-name="GitHub Actions deployer"

# Broad on purpose, for learning. In production, scope these down or split plan/apply identities.
for role in roles/viewer roles/serviceusage.serviceUsageAdmin roles/pubsub.admin roles/storage.admin \
  roles/bigquery.admin roles/iam.serviceAccountAdmin roles/iam.serviceAccountUser \
  roles/resourcemanager.projectIamAdmin roles/monitoring.admin roles/run.admin \
  roles/artifactregistry.admin; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${DEPLOYER_EMAIL}" --role="$role" --condition=None >/dev/null
done

echo "== Workload Identity Federation: trust tokens from your GitHub repo only"
gcloud iam workload-identity-pools describe "$POOL" --location=global >/dev/null 2>&1 ||
  gcloud iam workload-identity-pools create "$POOL" --location=global --display-name="GitHub"
gcloud iam workload-identity-pools providers describe "$PROVIDER" \
  --location=global --workload-identity-pool="$POOL" >/dev/null 2>&1 ||
  gcloud iam workload-identity-pools providers create-oidc "$PROVIDER" \
    --location=global --workload-identity-pool="$POOL" \
    --issuer-uri="https://token.actions.githubusercontent.com" \
    --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository" \
    --attribute-condition="assertion.repository=='${GITHUB_REPO}'"

gcloud iam service-accounts add-iam-policy-binding "$DEPLOYER_EMAIL" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/projects/${PROJECT_NUMBER}/locations/global/workloadIdentityPools/${POOL}/attribute.repository/${GITHUB_REPO}" >/dev/null

WIF_PROVIDER=$(gcloud iam workload-identity-pools providers describe "$PROVIDER" \
  --location=global --workload-identity-pool="$POOL" --format='value(name)')

cat <<OUT

Done. Add these as GitHub *repository variables* (Settings > Secrets and variables > Actions > Variables):

  GCP_PROJECT_ID   = ${PROJECT_ID}
  GCP_REGION       = ${REGION}
  TF_STATE_BUCKET  = ${STATE_BUCKET}
  WIF_PROVIDER     = ${WIF_PROVIDER}
  DEPLOYER_SA      = ${DEPLOYER_EMAIL}

Or, with the GitHub CLI:

  gh variable set GCP_PROJECT_ID  --repo ${GITHUB_REPO} --body "${PROJECT_ID}"
  gh variable set GCP_REGION      --repo ${GITHUB_REPO} --body "${REGION}"
  gh variable set TF_STATE_BUCKET --repo ${GITHUB_REPO} --body "${STATE_BUCKET}"
  gh variable set WIF_PROVIDER    --repo ${GITHUB_REPO} --body "${WIF_PROVIDER}"
  gh variable set DEPLOYER_SA     --repo ${GITHUB_REPO} --body "${DEPLOYER_EMAIL}"

No secrets are needed: nothing here is sensitive, and there is no key file.
OUT
