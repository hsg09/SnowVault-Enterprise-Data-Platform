# =============================================================================
# Module: Network Policies
# =============================================================================

terraform {
  required_providers {
    snowflake = {
      source = "Snowflake-Labs/snowflake"
    }
  }
}

variable "environment" {
  type = string
}

variable "allowed_ips" {
  type    = list(string)
  default = ["0.0.0.0/0"]
}

resource "snowflake_network_policy" "platform" {
  name            = "NP_${upper(var.environment)}_PLATFORM"
  allowed_ip_list = var.allowed_ips
  blocked_ip_list = []
  comment         = "${var.environment} network policy — restrict in production"
}
