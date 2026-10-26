output "bucket" {
  value = google_storage_bucket.landing.name
}

output "topic" {
  value = google_pubsub_topic.events.id
}

output "subscription" {
  value = one(google_pubsub_subscription.pull[*].id)
}

output "subscriber_service_account" {
  value = google_service_account.subscriber.email
}

output "upload_command" {
  value = "gcloud storage cp sample-data/orders.csv gs://${google_storage_bucket.landing.name}/incoming/"
}

output "artifact_registry" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.apps.repository_id}"
}
