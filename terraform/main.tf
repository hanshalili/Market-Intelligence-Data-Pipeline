# ==============================================================================
# main.tf — Apple & Tesla Market Intelligence Pipeline
# Provisions: GCS bucket, BigQuery dataset, and IAM service account
# ==============================================================================

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

# ------------------------------------------------------------------------------
# Service Account — used by Airflow and dbt to interact with GCP
# ------------------------------------------------------------------------------
resource "google_service_account" "pipeline_sa" {
  account_id   = "market-pipeline-sa"
  display_name = "Market Intelligence Pipeline Service Account"
  description  = "SA used by Airflow DAGs and dbt to read/write GCS and BigQuery"
}

# Grant the SA the roles it needs
locals {
  sa_roles = [
    "roles/storage.objectAdmin",      # Read/write GCS objects
    "roles/bigquery.dataEditor",      # Insert/update BQ tables
    "roles/bigquery.jobUser",         # Run BQ jobs
    "roles/bigquery.metadataViewer",  # Inspect BQ schemas
  ]
}

resource "google_project_iam_member" "pipeline_sa_roles" {
  for_each = toset(local.sa_roles)

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Create and export a JSON key for local development / Docker secrets
resource "google_service_account_key" "pipeline_sa_key" {
  service_account_id = google_service_account.pipeline_sa.name
  keepers = {
    # Rotate key by changing this value
    rotation = "2025-01"
  }
}

# Write the key to a local file (gitignored — for Docker volume mount)
resource "local_file" "sa_key_json" {
  content  = base64decode(google_service_account_key.pipeline_sa_key.private_key)
  filename = "${path.module}/../airflow/credentials/service_account.json"

  file_permission = "0600"
}

# ------------------------------------------------------------------------------
# GCS Data Lake Bucket
# ------------------------------------------------------------------------------
resource "google_storage_bucket" "data_lake" {
  name                        = "${var.project_id}-market-data-lake"
  location                    = var.region
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  force_destroy               = false  # Protect production data

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age                   = 90
      matches_storage_class = ["STANDARD"]
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  lifecycle_rule {
    condition {
      age = 365
    }
    action {
      type = "Delete"
    }
  }

  labels = {
    project     = "market-intelligence"
    environment = var.environment
    managed_by  = "terraform"
  }
}

# Pre-create the logical "folder" prefixes (GCS is flat but these aid clarity)
resource "google_storage_bucket_object" "raw_prefix_aapl" {
  name    = "raw/aapl/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = "placeholder"
}

resource "google_storage_bucket_object" "raw_prefix_tsla" {
  name    = "raw/tsla/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = "placeholder"
}

resource "google_storage_bucket_object" "raw_prefix_spy" {
  name    = "raw/spy/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = "placeholder"
}

resource "google_storage_bucket_object" "curated_prefix" {
  name    = "curated/stock_prices/.keep"
  bucket  = google_storage_bucket.data_lake.name
  content = "placeholder"
}

# ------------------------------------------------------------------------------
# BigQuery Dataset
# ------------------------------------------------------------------------------
resource "google_bigquery_dataset" "market_analytics" {
  dataset_id                 = var.bq_dataset
  friendly_name              = "Market Analytics"
  description                = "Partitioned and clustered tables for daily stock market analysis"
  location                   = var.bq_location
  delete_contents_on_destroy = false

  labels = {
    project     = "market-intelligence"
    environment = var.environment
    managed_by  = "terraform"
  }

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role          = "WRITER"
    user_by_email = google_service_account.pipeline_sa.email
  }
}

# ------------------------------------------------------------------------------
# BigQuery Tables — Terraform defines schema + partitioning; dbt populates data
# ------------------------------------------------------------------------------

# Staging table: raw, lightly-cleaned stock prices loaded by Airflow
resource "google_bigquery_table" "stg_stock_prices" {
  dataset_id          = google_bigquery_dataset.market_analytics.dataset_id
  table_id            = "raw_stock_prices"
  description         = "Staging table: daily adjusted stock prices loaded from GCS Parquet"
  deletion_protection = false

  # PARTITION by date improves query performance — BigQuery only scans
  # the relevant day partitions instead of the full table.
  time_partitioning {
    type          = "DAY"
    field         = "date"
    expiration_ms = null
  }

  # CLUSTER by symbol so queries filtered on AAPL/TSLA/SPY co-locate data,
  # reducing bytes scanned and lowering cost.
  clustering = ["symbol"]

  schema = file("${path.module}/schemas/stg_stock_prices.json")

  labels = {
    managed_by = "terraform"
    layer      = "staging"
  }
}

# Mart table: transformed metrics produced by dbt
resource "google_bigquery_table" "mart_daily_metrics" {
  dataset_id          = google_bigquery_dataset.market_analytics.dataset_id
  table_id            = "mart_daily_metrics"
  description         = "Analytics mart: financial metrics (SMA, returns, volatility, drawdown)"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "date"
  }

  clustering = ["symbol"]

  schema = file("${path.module}/schemas/mart_daily_metrics.json")

  labels = {
    managed_by = "terraform"
    layer      = "mart"
  }
}
