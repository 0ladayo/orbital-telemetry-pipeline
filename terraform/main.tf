resource "google_storage_bucket" "terraform_state" {
  name          = var.terraform_state_bucket_name
  location      = var.gcp_region
  public_access_prevention = "enforced"
  
  versioning {
    enabled = true
  }
}

resource "google_storage_bucket" "staging_bucket" {
  name          = var.staging_bucket_name
  location      = var.gcp_region
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
  dataset_id  = var.bigquery_dataset_id
  location    = var.gcp_region
}
