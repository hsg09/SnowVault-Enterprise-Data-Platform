# =============================================================================
# Module: RBAC — Outputs
# =============================================================================

output "role_names" {
  description = "Map of functional role names"
  value = {
    for k, v in snowflake_account_role.roles : k => v.name
  }
}

output "terraform_svc_user" {
  description = "Terraform service account username"
  value       = snowflake_user.terraform_svc.name
}

output "terraform_deployer_role" {
  description = "Least-privilege Terraform deployer role"
  value       = snowflake_account_role.terraform_deployer.name
}
