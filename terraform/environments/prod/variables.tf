# =============================================================================
# Variables — Production Environment
# =============================================================================

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "prod"
}

variable "snowflake_organization" {
  description = "Snowflake organization name"
  type        = string
}

variable "snowflake_account" {
  description = "Snowflake account locator (primary region)"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake service account user (TERRAFORM_SVC_USER)"
  type        = string
  default     = "TERRAFORM_SVC_USER"
}

variable "snowflake_private_key" {
  description = "RSA private key for key-pair auth (PEM format)"
  type        = string
  sensitive   = true
}

variable "snowflake_role" {
  description = "Snowflake role for Terraform operations (least-privilege)"
  type        = string
  default     = "TERRAFORM_DEPLOYER"
}

variable "aws_s3_role_arn" {
  description = "AWS IAM Role ARN for S3 storage integration"
  type        = string
}

variable "azure_tenant_id" {
  description = "Azure AD Tenant ID for ADLS Gen2 integration"
  type        = string
}

variable "gcs_allowed_locations" {
  description = "GCS bucket paths for storage integration"
  type        = list(string)
}

variable "secondary_accounts" {
  description = "List of secondary Snowflake accounts for cross-region/cross-cloud replication"
  type        = list(string)
}

variable "replication_schedule" {
  description = "CRON expression for replication refresh"
  type        = string
  default     = "USING CRON 0 */1 * * * UTC"
}

variable "allowed_ip_list" {
  description = "CIDR ranges for network policy (production — strictly restricted)"
  type        = list(string)
}

variable "credit_quota_monthly" {
  description = "Monthly credit quota for resource monitors"
  type        = number
  default     = 5000
}
