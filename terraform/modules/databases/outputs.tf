# =============================================================================
# Module: Databases — Outputs
# =============================================================================

output "database_names" {
  description = "Map of database names by purpose"
  value = {
    raw_vault      = snowflake_database.raw_vault.name
    business_vault = snowflake_database.business_vault.name
    analytics      = snowflake_database.analytics.name
    audit          = snowflake_database.audit.name
  }
}

output "raw_vault_name" {
  description = "Raw Vault database name"
  value       = snowflake_database.raw_vault.name
}

output "business_vault_name" {
  description = "Business Vault database name"
  value       = snowflake_database.business_vault.name
}

output "analytics_name" {
  description = "Analytics database name"
  value       = snowflake_database.analytics.name
}

output "audit_name" {
  description = "Audit database name"
  value       = snowflake_database.audit.name
}
