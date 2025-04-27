terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "6.16.0"
    }
  }
}

provider "google" {
  credentials = file(var.google_credential)
  project = var.project
  region  = var.region
}

# Raw data bucket
resource "google_storage_bucket" "data_lake" {
  name     = var.gcs_bucket_name
  location = var.region
  force_destroy = true


  lifecycle_rule {
    condition {
      age = 1
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# BigQuery dataset
resource "google_bigquery_dataset" "review_dataset" {
  dataset_id = var.bigquery_dataset
  location   = var.region
}

# Dataproc cluster
resource "google_dataproc_cluster" "spark_cluster" {
  name     = "amazon-review-cluster"
  project  = var.project
  region   = var.region

  cluster_config {
    staging_bucket = google_storage_bucket.data_lake.name

    master_config {
      num_instances = 1
      machine_type  = "n1-standard-4"
      disk_config {
        boot_disk_size_gb = 100
      }
    }

    worker_config {
      num_instances = 4
      machine_type  = "n1-standard-4"
      disk_config {
        boot_disk_size_gb = 100
      }
    }

    software_config {
      image_version = "2.1-debian11"  # Dataproc image with Spark
      optional_components = ["JUPYTER", "DOCKER"]
    }

    endpoint_config {
      enable_http_port_access = true
    }

    gce_cluster_config {
      zone = "${var.region}-b"

      service_account = var.service_account_email
      service_account_scopes = [
        "cloud-platform"
      ]

      tags = ["dataproc-cluster"]
    }
  }
}

