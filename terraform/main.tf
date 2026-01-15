resource "google_storage_bucket" "terraform_state" {
  name                     = var.terraform_state_bucket_name
  location                 = var.gcp_region
  public_access_prevention = "enforced"

  versioning {
    enabled = true
  }
}

resource "google_storage_bucket" "staging_bucket" {
  name                     = var.staging_bucket_name
  location                 = var.gcp_region
  public_access_prevention = "enforced"

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 2
    }

  }
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bigquery_dataset_id
  location   = var.gcp_region
}

resource "google_service_account" "service_account" {
  account_id   = var.service_account_id
  display_name = "Telemetry Pipeline Service Account"
}

resource "google_storage_bucket_iam_member" "pipeline_sa_storage_access" {
  bucket = google_storage_bucket.staging_bucket.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.service_account.email}"
}

resource "google_project_iam_member" "pipeline_sa_data_editor" {
  project = var.gcp_project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}
resource "google_project_iam_member" "pipeline_sa_bq_job_user" {
  project = var.gcp_project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}

resource "google_project_service" "required_apis" {
  for_each = toset([
    "cloudbuild.googleapis.com", "secretmanager.googleapis.com",
    "cloudfunctions.googleapis.com", "run.googleapis.com",
    "artifactregistry.googleapis.com", "cloudresourcemanager.googleapis.com",
    "cloudscheduler.googleapis.com"
  ])
  service            = each.key
  disable_on_destroy = false
}

resource "google_project_iam_member" "pipeline_sa_logging" {
  project = var.gcp_project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}

resource "google_cloudbuild_trigger" "ingestion_build_trigger" {
  location        = var.gcp_region
  name            = "ingestion-build-trigger"
  filename        = "ingestion/ingestion.cloudbuild.yaml"
  service_account = google_service_account.service_account.id

  included_files = ["ingestion/**"]

  repository_event_config {
    repository = "projects/${var.gcp_project_id}/locations/${var.gcp_region}/connections/github-connection/repositories/0ladayo-orbital-telemetry-pipeline"
    push {
      branch = "^main$"
    }
  }

  substitutions = {
    _LOCATION              = var.gcp_region
    _PROJECT_ID            = var.gcp_project_id
    _SERVICE_ACCOUNT_EMAIL = google_service_account.service_account.email
    _BUCKET_URL            = "gs://${google_storage_bucket.staging_bucket.name}"
    _PUBSUB_TOPIC          = var.pubsub_topic
  }

}

resource "google_project_iam_member" "pipeline_sa_functions_developer" {
  project = var.gcp_project_id
  role    = "roles/cloudfunctions.developer"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}

resource "google_project_iam_member" "pipeline_sa_user" {
  project = var.gcp_project_id
  role    = "roles/iam.serviceAccountUser"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}

resource "google_cloud_scheduler_job" "job" {
  name             = "ingestion-cron-job"
  schedule         = "0 0 * * *"
  time_zone        = "Europe/London"
  attempt_deadline = "320s"

  retry_config {
    retry_count = 1
  }

  http_target {
    http_method = "GET"
    uri         = "https://${var.gcp_region}-${var.gcp_project_id}.cloudfunctions.net/orbital-telemetry-pipeline/"
    oidc_token {
      service_account_email = google_service_account.service_account.email
      audience              = "https://${var.gcp_region}-${var.gcp_project_id}.cloudfunctions.net/orbital-telemetry-pipeline"
    }
    headers = {
      "Content-Type" = "application/json"
    }
  }
}

resource "google_project_iam_member" "scheduler_invoker" {
  project = var.gcp_project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.service_account.email}"
}

resource "google_pubsub_topic" "ingestion_complete_topic" {
  name = var.pubsub_topic

  labels = {
    project   = "space-traffic-analytics"
    stage     = "ingestion"
    data_type = "orbital-elements"
  }

  message_retention_duration = "86400s"
}

resource "google_pubsub_topic_iam_member" "member" {
  project = var.gcp_project_id
  topic = google_pubsub_topic.ingestion_complete_topic.name
  role = "roles/pubsub.publisher"
  member = "serviceAccount:${google_service_account.service_account.email}"
}

resource "google_cloudbuild_trigger" "transformation_build_trigger" {
  location        = var.gcp_region
  name            = "transformation-build-trigger"
  filename        = "transformation/transformation.cloudbuild.yaml"
  service_account = google_service_account.service_account.id

  included_files = ["transformation/**"]

  repository_event_config {
    repository = "projects/${var.gcp_project_id}/locations/${var.gcp_region}/connections/github-connection/repositories/0ladayo-orbital-telemetry-pipeline"
    push {
      branch = "^main$"
    }
  }

  substitutions = {
    _LOCATION              = var.gcp_region
    _PROJECT_ID            = var.gcp_project_id
    _SERVICE_ACCOUNT_EMAIL = google_service_account.service_account.email
    _BIGQUERY_DATASET      = google_bigquery_dataset.dataset.dataset_id
    _PUBSUB_TOPIC          = var.pubsub_topic
  }

}