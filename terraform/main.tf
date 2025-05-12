# Provider Configuration
terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "5.6.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Data Infrastructure
resource "google_storage_bucket" "raw_data" {
  name     = "${var.project_id}-${var.gcs_raw_bucket_name}"
  location = var.region

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
  force_destroy = true
}

resource "google_storage_bucket" "processed_data" {
  name     = "${var.project_id}-${var.gcs_processed_bucket_name}"
  location = var.region

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
  force_destroy = true
}

resource "google_bigquery_dataset" "crashes" {
  dataset_id = var.bq_dataset_name
  location   = var.region
}

# resource "google_dataproc_cluster" "spark_cluster" {
#   name   = "ccrs-spark-cluster"
#   region = var.region

#   cluster_config {
#     master_config {
#       num_instances = 1
#       machine_type  = "n1-standard-2"
#     }

#     worker_config {
#       num_instances = 2
#       machine_type  = "n1-standard-2"
#     }

#     software_config {
#       image_version = "2.1-debian10"
#       optional_components = ["ANACONDA", "JUPYTER"]
#     }
#   }
# }
