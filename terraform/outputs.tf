output "raw_bucket" {
  value = google_storage_bucket.raw_data.name
}

output "processed_bucket" {
  value = google_storage_bucket.processed_data.name
}

output "bq_dataset" {
  value = google_bigquery_dataset.crashes.dataset_id
}