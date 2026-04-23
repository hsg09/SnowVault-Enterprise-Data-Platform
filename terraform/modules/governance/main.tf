# =============================================================================
# Module: Governance — Snowflake Horizon Catalog (Masking, RAPs, Tags)
# Policies are defined as IaC and replicate across all regions via
# Failover Groups. This eliminates manual SQL-based policy management.
# =============================================================================

terraform {
  required_providers {
    snowflake = {
      source = "Snowflake-Labs/snowflake"
    }
  }
}

# =============================================================================
# 1. OBJECT TAGS — 5 Tag Types for Classification & Governance
# =============================================================================

resource "snowflake_tag" "pii" {
  database = "RAW_VAULT"
  schema   = "GOVERNANCE"
  name     = "PII"
  allowed_values = ["EMAIL", "PHONE", "SSN", "NAME", "ADDRESS", "DOB", "NONE"]
  comment  = "PII classification — drives automatic masking policy assignment"
}

resource "snowflake_tag" "sensitivity_level" {
  database = "RAW_VAULT"
  schema   = "GOVERNANCE"
  name     = "SENSITIVITY_LEVEL"
  allowed_values = ["PUBLIC", "INTERNAL", "CONFIDENTIAL", "RESTRICTED"]
  comment  = "Data sensitivity tier — governs access and masking behaviour"
}

resource "snowflake_tag" "data_domain" {
  database = "RAW_VAULT"
  schema   = "GOVERNANCE"
  name     = "DATA_DOMAIN"
  allowed_values = ["CUSTOMER", "ORDER", "PRODUCT", "FINANCIAL", "MARKETING", "OPERATIONAL"]
  comment  = "Business domain classification — used for lineage and ownership"
}

resource "snowflake_tag" "cost_center" {
  database = "RAW_VAULT"
  schema   = "GOVERNANCE"
  name     = "COST_CENTER"
  allowed_values = ["ENGINEERING", "DATA_SCIENCE", "BI_ANALYTICS", "FINANCE", "MARKETING"]
  comment  = "Cost center tag — drives credit usage attribution"
}

resource "snowflake_tag" "retention_class" {
  database = "RAW_VAULT"
  schema   = "GOVERNANCE"
  name     = "RETENTION_CLASS"
  allowed_values = ["EPHEMERAL", "SHORT_TERM", "STANDARD", "LONG_TERM", "REGULATORY"]
  comment  = "Retention class — governs Time Travel and fail-safe retention"
}

# =============================================================================
# 2. DYNAMIC DATA MASKING POLICIES — 6 PII Types
# =============================================================================

resource "snowflake_masking_policy" "mask_email" {
  database = "RAW_VAULT"
  schema   = "GOVERNANCE"
  name     = "MASK_EMAIL"
  signature {
    column {
      name = "VAL"
      type = "VARCHAR"
    }
  }
  masking_expression = <<-EOT
    CASE
      WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'PLATFORM_ADMIN', 'ACCOUNTADMIN', 'DATA_STEWARD')
        THEN VAL
      ELSE REGEXP_REPLACE(VAL, '^(.{2})(.*)(@.*)$', '\\1***\\3')
    END
  EOT
  return_data_type = "VARCHAR"
  comment          = "Email masking — shows first 2 chars + domain to non-privileged roles"
}

resource "snowflake_masking_policy" "mask_phone" {
  database = "RAW_VAULT"
  schema   = "GOVERNANCE"
  name     = "MASK_PHONE"
  signature {
    column {
      name = "VAL"
      type = "VARCHAR"
    }
  }
  masking_expression = <<-EOT
    CASE
      WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'PLATFORM_ADMIN', 'ACCOUNTADMIN', 'DATA_STEWARD')
        THEN VAL
      ELSE CONCAT('***-***-', RIGHT(VAL, 4))
    END
  EOT
  return_data_type = "VARCHAR"
  comment          = "Phone masking — shows last 4 digits to non-privileged roles"
}

resource "snowflake_masking_policy" "mask_name" {
  database = "RAW_VAULT"
  schema   = "GOVERNANCE"
  name     = "MASK_NAME"
  signature {
    column {
      name = "VAL"
      type = "VARCHAR"
    }
  }
  masking_expression = <<-EOT
    CASE
      WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'PLATFORM_ADMIN', 'ACCOUNTADMIN', 'DATA_STEWARD')
        THEN VAL
      ELSE CONCAT(LEFT(VAL, 1), '****')
    END
  EOT
  return_data_type = "VARCHAR"
  comment          = "Name masking — shows first initial to non-privileged roles"
}

resource "snowflake_masking_policy" "mask_ssn" {
  database = "RAW_VAULT"
  schema   = "GOVERNANCE"
  name     = "MASK_SSN"
  signature {
    column {
      name = "VAL"
      type = "VARCHAR"
    }
  }
  masking_expression = <<-EOT
    CASE
      WHEN CURRENT_ROLE() IN ('PLATFORM_ADMIN', 'ACCOUNTADMIN')
        THEN VAL
      ELSE '***-**-****'
    END
  EOT
  return_data_type = "VARCHAR"
  comment          = "SSN masking — full redaction except PLATFORM_ADMIN"
}

resource "snowflake_masking_policy" "mask_dob" {
  database = "RAW_VAULT"
  schema   = "GOVERNANCE"
  name     = "MASK_DOB"
  signature {
    column {
      name = "VAL"
      type = "DATE"
    }
  }
  masking_expression = <<-EOT
    CASE
      WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'PLATFORM_ADMIN', 'ACCOUNTADMIN', 'DATA_STEWARD')
        THEN VAL
      ELSE DATE_FROM_PARTS(YEAR(VAL), 1, 1)
    END
  EOT
  return_data_type = "DATE"
  comment          = "DOB masking — shows year only (Jan 1) to non-privileged roles"
}

resource "snowflake_masking_policy" "mask_address" {
  database = "RAW_VAULT"
  schema   = "GOVERNANCE"
  name     = "MASK_ADDRESS"
  signature {
    column {
      name = "VAL"
      type = "VARCHAR"
    }
  }
  masking_expression = <<-EOT
    CASE
      WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'PLATFORM_ADMIN', 'ACCOUNTADMIN', 'DATA_STEWARD')
        THEN VAL
      ELSE '*** REDACTED ***'
    END
  EOT
  return_data_type = "VARCHAR"
  comment          = "Address masking — full redaction for non-privileged roles"
}

# =============================================================================
# 3. ROW ACCESS POLICIES — 2 Data Filtering Policies
# =============================================================================

resource "snowflake_row_access_policy" "country_filter" {
  database = "RAW_VAULT"
  schema   = "GOVERNANCE"
  name     = "RAP_COUNTRY_FILTER"
  signature = {
    COUNTRY_CODE = "VARCHAR"
  }
  row_access_expression = <<-EOT
    CASE
      WHEN CURRENT_ROLE() IN ('PLATFORM_ADMIN', 'DATA_ENGINEER', 'ACCOUNTADMIN', 'DATA_STEWARD', 'TRANSFORMER')
        THEN TRUE
      WHEN CURRENT_ROLE() = 'ANALYST'
        AND COUNTRY_CODE IN (
          SELECT VALUE FROM TABLE(FLATTEN(
            INPUT => PARSE_JSON(COALESCE(CURRENT_SESSION()::VARIANT:allowed_countries, '["ALL"]'))
          ))
        )
        THEN TRUE
      ELSE FALSE
    END
  EOT
  comment = "Row Access Policy: Country-based data filtering for GDPR/data residency"
}

resource "snowflake_row_access_policy" "sensitivity_filter" {
  database = "RAW_VAULT"
  schema   = "GOVERNANCE"
  name     = "RAP_SENSITIVITY_FILTER"
  signature = {
    SENSITIVITY_LEVEL = "VARCHAR"
  }
  row_access_expression = <<-EOT
    CASE
      WHEN CURRENT_ROLE() IN ('PLATFORM_ADMIN', 'DATA_STEWARD', 'ACCOUNTADMIN', 'DATA_ENGINEER')
        THEN TRUE
      WHEN CURRENT_ROLE() = 'ANALYST'
        AND SENSITIVITY_LEVEL IN ('PUBLIC', 'INTERNAL')
        THEN TRUE
      ELSE FALSE
    END
  EOT
  comment = "Row Access Policy: Sensitivity-based filtering — CONFIDENTIAL restricted"
}
