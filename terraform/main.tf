# Learning-oriented Terraform for: GCS -> Pub/Sub -> subscriber -> BigQuery (+ DLQ).
# Use this OR scripts/setup.sh, not both: they create the same resource names.
# Not included: the Cloud Run push service. It is an application deployment, handled by
# scripts/deploy_push.sh (run by the CD workflow), not by Terraform.

data "google_project" "this" {
  project_id = var.project_id
}

locals {
  # Service agents are Google-managed identities that act on your behalf.
  pubsub_agent = "serviceAccount:service-${data.google_project.this.number}@gcp-sa-pubsub.iam.gserviceaccount.com"
}

# ---------- APIs ----------
resource "google_project_service" "apis" {
  for_each = toset([
    "pubsub.googleapis.com",
    "storage.googleapis.com",
    "bigquery.googleapis.com",
    "monitoring.googleapis.com",
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
  ])
  service            = each.key
  disable_on_destroy = false
}

# ---------- Source: bucket ----------
resource "google_storage_bucket" "landing" {
  name                        = "${var.project_id}-landing"
  location                    = var.bq_location
  uniform_bucket_level_access = true
  force_destroy               = true # learning only: lets `destroy` delete a non-empty bucket
  depends_on                  = [google_project_service.apis]
}

# ---------- Messaging: topics ----------
resource "google_pubsub_topic" "events" {
  name       = "file-events"
  depends_on = [google_project_service.apis]
}

resource "google_pubsub_topic" "dlq" {
  name       = "file-events-dlq"
  depends_on = [google_project_service.apis]
}

# Cloud Storage must be allowed to publish, otherwise notifications fail silently.
data "google_storage_project_service_account" "gcs" {}

resource "google_pubsub_topic_iam_member" "gcs_can_publish" {
  topic  = google_pubsub_topic.events.id
  role   = "roles/pubsub.publisher"
  member = "serviceAccount:${data.google_storage_project_service_account.gcs.email_address}"
}

# New object under incoming/ -> message on the topic.
resource "google_storage_notification" "on_upload" {
  bucket             = google_storage_bucket.landing.name
  topic              = google_pubsub_topic.events.id
  payload_format     = "JSON_API_V1"
  event_types        = ["OBJECT_FINALIZE"]
  object_name_prefix = "incoming/"
  depends_on         = [google_pubsub_topic_iam_member.gcs_can_publish]
}

# ---------- Messaging: subscriptions ----------
resource "google_pubsub_subscription" "pull" {
  count = var.enable_pull_subscription ? 1 : 0

  name                       = "file-events-sub"
  topic                      = google_pubsub_topic.events.id
  ack_deadline_seconds       = 60
  message_retention_duration = "604800s" # 7 days
  retain_acked_messages      = true      # needed to replay with `seek`

  expiration_policy {
    ttl = "" # never expire (the default deletes a subscription idle for 31 days)
  }

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.dlq.id
    max_delivery_attempts = 5
  }
}

# Lets you pull failed messages and inspect them.
resource "google_pubsub_subscription" "dlq_inspect" {
  name  = "file-events-dlq-sub"
  topic = google_pubsub_topic.dlq.id

  expiration_policy {
    ttl = "" # never expire
  }
}

# Dead-lettering is performed by the Pub/Sub agent, so it needs these two roles.
resource "google_pubsub_topic_iam_member" "agent_publishes_dlq" {
  topic  = google_pubsub_topic.dlq.id
  role   = "roles/pubsub.publisher"
  member = local.pubsub_agent
}

resource "google_pubsub_subscription_iam_member" "agent_acks_source" {
  count = var.enable_pull_subscription ? 1 : 0

  subscription = google_pubsub_subscription.pull[0].name
  role         = "roles/pubsub.subscriber"
  member       = local.pubsub_agent
}

# ---------- Target: BigQuery ----------
resource "google_bigquery_dataset" "raw" {
  dataset_id                 = "raw"
  location                   = var.bq_location
  delete_contents_on_destroy = true # learning only
  depends_on                 = [google_project_service.apis]
}

resource "google_bigquery_table" "orders" {
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "orders"
  deletion_protection = false # learning only
  schema = jsonencode([
    { name = "order_id", type = "STRING" },
    { name = "customer_id", type = "STRING" },
    { name = "amount", type = "FLOAT" },
    { name = "created_at", type = "TIMESTAMP" },
  ])
}

# ---------- Identity for the subscriber, least privilege ----------
resource "google_service_account" "subscriber" {
  account_id   = "ingest-subscriber"
  display_name = "Ingest subscriber"
}

locals {
  subscriber = "serviceAccount:${google_service_account.subscriber.email}"
}

resource "google_pubsub_subscription_iam_member" "sub_can_pull" {
  count = var.enable_pull_subscription ? 1 : 0

  subscription = google_pubsub_subscription.pull[0].name
  role         = "roles/pubsub.subscriber"
  member       = local.subscriber
}

resource "google_storage_bucket_iam_member" "sub_can_read" {
  bucket = google_storage_bucket.landing.name
  role   = "roles/storage.objectViewer"
  member = local.subscriber
}

resource "google_project_iam_member" "sub_runs_bq_jobs" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = local.subscriber
}

resource "google_bigquery_table_iam_member" "sub_writes_table" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = google_bigquery_table.orders.table_id
  role       = "roles/bigquery.dataEditor"
  member     = local.subscriber
}

# ---------- Container registry for the push service image ----------
resource "google_artifact_registry_repository" "apps" {
  repository_id = "apps"
  location      = var.region
  format        = "DOCKER"
  description   = "Images for the ingestion pipeline"
  depends_on    = [google_project_service.apis]
}
