# =============================================================================
# Module: Storage Integrations — Outputs
# =============================================================================

output "integration_names" {
  description = "Map of storage integration names by cloud provider"
  value = {
    s3   = var.aws_s3_role_arn != "" ? "S3_RAW_INTEGRATION" : null
    adls = var.azure_tenant_id != "" ? "ADLS_RAW_INTEGRATION" : null
    gcs  = length(var.gcs_allowed_locations) > 0 ? "GCS_RAW_INTEGRATION" : null
  }
}
