terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~>7.14.1"
    }
  }
  backend "gcs" {
    bucket = "orbital-telemetry-state"
    prefix = "terraform/state"

  }
}