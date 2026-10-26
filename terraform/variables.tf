variable "project_id" {
  description = "GCP project that will host the pipeline."
  type        = string
}

variable "region" {
  description = "Default region for the provider."
  type        = string
  default     = "europe-west9"
}

variable "bq_location" {
  description = "Location of the bucket and BigQuery dataset. Load jobs need them to match."
  type        = string
  default     = "EU"
}

variable "notification_channels" {
  description = "Optional Cloud Monitoring notification channel IDs for the backlog alert."
  type        = list(string)
  default     = []
}

variable "enable_pull_subscription" {
  description = "Create the pull subscription used by a locally-run subscriber. CD sets this to false and relies on the Cloud Run push subscription."
  type        = bool
  default     = true
}
