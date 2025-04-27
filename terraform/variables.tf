variable "google_credential" {
  description = "gcp service account credential"
  default = "/home/vice/DEZ/amazon_product_review/key/de-amazon-product-review-88dd24afade6.json"
}

variable "project" {
  description = "project name"
  default = "de-amazon-product-review"
}

variable "region" {
  description = "GCP region"
  default = "australia-southeast1"
}

variable "gcs_bucket_name" {
  default     = "de-amazon-product-review-bucket"
}

variable "bigquery_dataset" {
  default = "de_amazon_product_reviews"
}


variable "service_account_email" {
  type        = string
  description = "Service Account email to attach to Dataproc Cluster"
  default     = "jimmyh@de-amazon-product-review.iam.gserviceaccount.com"
}

