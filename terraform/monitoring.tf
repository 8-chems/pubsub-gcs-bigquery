# Dashboard: same JSON that `gcloud monitoring dashboards create` accepts.
resource "google_monitoring_dashboard" "pipeline" {
  dashboard_json = file("${path.module}/../dashboards/pubsub-dashboard.json")
  depends_on     = [google_project_service.apis]
}

# Alert when a message has waited unacknowledged for more than 5 minutes,
# the classic sign of a stuck or crashed subscriber.
resource "google_monitoring_alert_policy" "backlog" {
  display_name = "Pub/Sub backlog: oldest unacked message over 5 min"
  combiner     = "OR"

  conditions {
    display_name = "oldest_unacked_message_age above 300s"
    condition_threshold {
      filter          = "metric.type=\"pubsub.googleapis.com/subscription/oldest_unacked_message_age\" AND resource.type=\"pubsub_subscription\" AND resource.label.subscription_id=monitoring.regex.full_match(\"file-events-(sub|push-sub)\")"
      comparison      = "COMPARISON_GT"
      threshold_value = 300
      duration        = "300s"
      aggregations {
        alignment_period   = "60s"
        per_series_aligner = "ALIGN_MAX"
      }
    }
  }

  notification_channels = var.notification_channels
}
