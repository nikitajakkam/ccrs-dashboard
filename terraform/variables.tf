# General Project Setup
variable "project_id" {
  description = "Project ID"
  default     = "ccrs-dashboard"
}

variable "region" {
  description = "Region"
  default     = "northamerica-northeast1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

# BigQuery
variable "bq_dataset_name" {
  description = "BigQuery Dataset Name"
  default     = "crashes"
}

# GCS
variable "gcs_raw_bucket_name" {
  description = "Raw Data Storage Bucket Name"
  default     = "raw-data"
}

variable "gcs_processed_bucket_name" {
  description = "Processed Data Storage Bucket Name"
  default     = "processed-data"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}