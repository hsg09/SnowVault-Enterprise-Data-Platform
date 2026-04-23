# =============================================================================
# Module: Network Policies — Variables
# =============================================================================

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
}

variable "allowed_ips" {
  description = "CIDR ranges for network policy"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}
