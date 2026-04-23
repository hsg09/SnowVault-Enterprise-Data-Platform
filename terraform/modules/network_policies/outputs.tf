# =============================================================================
# Module: Network Policies — Outputs
# =============================================================================

output "policy_name" {
  description = "Network policy name"
  value       = snowflake_network_policy.platform.name
}
