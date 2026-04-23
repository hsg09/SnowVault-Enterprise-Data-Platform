# =============================================================================
# Module: Warehouses — Outputs
# =============================================================================

output "warehouse_names" {
  description = "Map of warehouse names by workload"
  value = {
    ingestion   = snowflake_warehouse.ingestion.name
    transformer = snowflake_warehouse.transformer.name
    analytics   = snowflake_warehouse.analytics.name
    dev         = snowflake_warehouse.dev.name
    ci          = snowflake_warehouse.ci.name
  }
}
