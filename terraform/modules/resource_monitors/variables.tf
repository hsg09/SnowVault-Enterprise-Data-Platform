# =============================================================================
# Module: Resource Monitors — Variables
# =============================================================================

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "credit_quota_monthly" {
  description = "Account-level monthly credit quota"
  type        = number
  default     = 100
}
