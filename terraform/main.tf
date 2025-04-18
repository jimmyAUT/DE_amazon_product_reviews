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


resource "google_bigquery_dataset" "review_dataset" {
  dataset_id = var.bigquery_dataset
  location   = var.region
}


