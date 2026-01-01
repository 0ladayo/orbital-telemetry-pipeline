variable "gcp_project_id" {
  type        = string
  description = "The ID of the Google Cloud project where resources will be deployed."

}

variable "gcp_region" {
  type        = string
  description = "The default GCP region for resources"

}

variable "gcp_zone" {
  type        = string
  description = "The default GCP zone for resources"

}

variable "terraform_state_bucket_name" {
  type        = string
  description = "The name of the GCS bucket used to store the Terraform remote state."

}

variable "staging_bucket_name" {
  type        = string
  description = "The name of the GCS bucket used to store the extracted data"

}

variable "bigquery_dataset_id" {
  type        = string
  description = "The ID of the BigQuery dataset to store the extracted telemetry tables"

}

variable "service_account_id" {
  type        = string
  description = "The ID for the service account used by the data ingestion pipeline."

}