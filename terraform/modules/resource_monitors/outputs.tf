# =============================================================================
# Module: Resource Monitors — Outputs
# =============================================================================

output "monitor_names" {
  description = "Map of resource monitor names"
  value = {
    account     = snowflake_resource_monitor.account_global.name
    ingestion   = snowflake_resource_monitor.ingestion.name
    transformer = snowflake_resource_monitor.transformer.name
    analytics   = snowflake_resource_monitor.analytics.name
    ci          = snowflake_resource_monitor.ci.name
    dev         = snowflake_resource_monitor.dev.name
  }
}
