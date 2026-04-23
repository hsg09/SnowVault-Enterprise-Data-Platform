# =============================================================================
# Module: Replication — Variables
# =============================================================================

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "secondary_accounts" {
  description = "List of secondary Snowflake accounts for replication"
  type        = list(string)
  default     = []
}

variable "replication_schedule" {
  description = "CRON expression for replication refresh"
  type        = string
  default     = "USING CRON 0 */2 * * * UTC"
}
