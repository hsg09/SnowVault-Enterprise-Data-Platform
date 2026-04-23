# =============================================================================
# Module: Storage Integrations — Multi-Cloud (AWS S3, Azure ADLS, GCP GCS)
# Blueprint Pipelines 1-3: Object Storage Ingestion
# =============================================================================

terraform {
  required_providers {
    snowflake = {
      source = "Snowflake-Labs/snowflake"
    }
  }
}

# =============================================================================
# Pipeline 1: AWS S3 Storage Integration
# =============================================================================

resource "snowflake_storage_integration" "s3_raw" {
  count   = var.aws_s3_role_arn != "" ? 1 : 0
  name    = "S3_RAW_INTEGRATION"
  type    = "EXTERNAL_STAGE"
  enabled = true

  storage_provider     = "S3"
  storage_aws_role_arn = var.aws_s3_role_arn

  storage_allowed_locations = [
    "s3://your-company-data-lake-raw/",
    "s3://your-company-data-lake-raw-eu/",
  ]

  storage_blocked_locations = [
    "s3://your-company-data-lake-raw/restricted/",
  ]

  comment = "Pipeline 1: AWS S3 — Primary data lake (IAM trust relationship)"
}

# =============================================================================
# Pipeline 2: Azure ADLS Gen2 Storage Integration
# =============================================================================

resource "snowflake_storage_integration" "adls_raw" {
  count   = var.azure_tenant_id != "" ? 1 : 0
  name    = "ADLS_RAW_INTEGRATION"
  type    = "EXTERNAL_STAGE"
  enabled = true

  storage_provider  = "AZURE"
  azure_tenant_id   = var.azure_tenant_id

  storage_allowed_locations = [
    "azure://yourstorageaccount.blob.core.windows.net/raw/",
    "azure://yourstorageaccount.blob.core.windows.net/raw-eu/",
  ]

  comment = "Pipeline 2: Azure ADLS Gen2 — Secondary data lake (managed identity)"
}

# =============================================================================
# Pipeline 3: GCP GCS Storage Integration
# =============================================================================

resource "snowflake_storage_integration" "gcs_raw" {
  count   = length(var.gcs_allowed_locations) > 0 ? 1 : 0
  name    = "GCS_RAW_INTEGRATION"
  type    = "EXTERNAL_STAGE"
  enabled = true

  storage_provider = "GCS"

  storage_allowed_locations = var.gcs_allowed_locations

  comment = "Pipeline 3: GCP GCS — Tertiary data lake (service account)"
}

