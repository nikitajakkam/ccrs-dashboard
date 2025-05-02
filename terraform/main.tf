provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_storage_bucket" "raw_data" {
  name     = "${var.project_id}-ccrs-raw"
  location = var.region
}

resource "google_storage_bucket" "processed_data" {
  name     = "${var.project_id}-ccrs-processed"
  location = var.region
}

resource "google_bigquery_dataset" "crashes" {
  dataset_id = "ccrs_crashes"
  location   = var.region
}

resource "google_dataproc_cluster" "spark_cluster" {
  name   = "ccrs-spark-cluster"
  region = var.region

  cluster_config {
    master_config {
      num_instances = 1
      machine_type  = "n1-standard-2"
    }

    worker_config {
      num_instances = 2
      machine_type  = "n1-standard-2"
    }

    software_config {
      image_version = "2.1-debian10"
      optional_components = ["ANACONDA", "JUPYTER"]
    }
  }
}
