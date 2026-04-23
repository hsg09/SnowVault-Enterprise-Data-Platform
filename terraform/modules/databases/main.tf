# =============================================================================
# Module: Databases — 4-Database Medallion + Data Vault Architecture
# =============================================================================

terraform {
  required_providers {
    snowflake = {
      source = "Snowflake-Labs/snowflake"
    }
  }
}

locals {
  env_suffix = var.environment == "prod" ? "" : "_${upper(var.environment)}"
}

# --- RAW_VAULT (Bronze: Raw Data Vault) ---
resource "snowflake_database" "raw_vault" {
  name                        = "RAW_VAULT${local.env_suffix}"
  data_retention_time_in_days = var.environment == "prod" ? 90 : 1
  comment                     = "Bronze — Raw Data Vault: immutable source data landing zone"
}

resource "snowflake_schema" "raw_vault_schemas" {
  for_each = toset(["ECOMMERCE", "CRM", "STREAMING", "CDC", "GOVERNANCE", "RAW_VAULT"])
  database = snowflake_database.raw_vault.name
  name     = each.key
}

# --- BUSINESS_VAULT (Silver: Business Vault) ---
resource "snowflake_database" "business_vault" {
  name                        = "BUSINESS_VAULT${local.env_suffix}"
  data_retention_time_in_days = var.environment == "prod" ? 90 : 1
  comment                     = "Silver — Business Vault: PIT, Bridge, Conformed integration layer"
}

resource "snowflake_schema" "business_vault_schemas" {
  for_each = toset(["BUSINESS_VAULT", "PIT_TABLES", "BRIDGE_TABLES", "CONFORMED"])
  database = snowflake_database.business_vault.name
  name     = each.key
}

# --- ANALYTICS (Gold: Star Schema / Reporting) ---
resource "snowflake_database" "analytics" {
  name                        = "ANALYTICS${local.env_suffix}"
  data_retention_time_in_days = var.environment == "prod" ? 90 : 1
  comment                     = "Gold — Analytics: Facts, Dimensions, Aggregates, Semantic Views"
}

resource "snowflake_schema" "analytics_schemas" {
  for_each = toset(["FACTS", "DIMENSIONS", "AGGREGATES", "SEMANTIC_VIEWS", "SECURE_VIEWS"])
  database = snowflake_database.analytics.name
  name     = each.key
}

# --- AUDIT (Control Plane) ---
resource "snowflake_database" "audit" {
  name                        = "AUDIT${local.env_suffix}"
  data_retention_time_in_days = var.environment == "prod" ? 365 : 7
  comment                     = "Audit — Control plane: ingestion logs, DQ results, cost attribution"
}

resource "snowflake_schema" "audit_schemas" {
  for_each = toset(["CONTROL", "DQ_RESULTS", "COST_ATTRIBUTION", "SCHEMA_REGISTRY"])
  database = snowflake_database.audit.name
  name     = each.key
}

