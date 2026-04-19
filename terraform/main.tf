terraform {
  required_version = ">= 1.5.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "vic_vehicle_lake" {
  name                        = var.bucket_name
  location                    = var.bucket_location
  storage_class               = var.storage_class
  force_destroy               = true
  uniform_bucket_level_access = true

  labels = {
    env     = "dev"
    owner   = "amrith"
    project = "vic-vehicle-analytics"
  }

  versioning {
    enabled = true
  }
}

resource "google_bigquery_dataset" "vic_vehicle_analytics" {
  dataset_id                 = var.bq_dataset
  location                   = var.bq_location
  delete_contents_on_destroy = true

  labels = {
    env     = "dev"
    project = "vic_vehicle_analytics"
  }
}

output "gcs_bucket_name" {
  value = google_storage_bucket.vic_vehicle_lake.name
}

output "bigquery_dataset_id" {
  value = google_bigquery_dataset.vic_vehicle_analytics.dataset_id
}