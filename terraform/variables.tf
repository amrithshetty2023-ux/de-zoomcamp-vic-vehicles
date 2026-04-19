variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "Default GCP region for provider"
  type        = string
  default     = "us-central1"
}

variable "bucket_name" {
  description = "Name of the GCS data lake bucket"
  type        = string
}

variable "bucket_location" {
  description = "Location for GCS bucket"
  type        = string
  default     = "US"
}

variable "storage_class" {
  description = "Storage class for GCS bucket"
  type        = string
  default     = "STANDARD"
}

variable "bq_dataset" {
  description = "BigQuery dataset name"
  type        = string
}

variable "bq_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "US"
}