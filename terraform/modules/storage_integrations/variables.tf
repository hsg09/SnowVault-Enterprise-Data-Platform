# =============================================================================
# Module: Storage Integrations — Variables
# =============================================================================

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "aws_s3_role_arn" {
  description = "AWS IAM Role ARN for S3 storage integration"
  type        = string
  default     = ""
}

variable "azure_tenant_id" {
  description = "Azure AD Tenant ID for ADLS Gen2 integration"
  type        = string
  default     = ""
}

variable "gcs_allowed_locations" {
  description = "GCS bucket paths for storage integration"
  type        = list(string)
  default     = []
}
