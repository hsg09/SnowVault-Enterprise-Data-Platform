# =============================================================================
# Module: RBAC — Hierarchical Role-Based Access Control
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

# --- Functional Roles ---
resource "snowflake_account_role" "roles" {
  for_each = toset([
    "LOADER",
    "TRANSFORMER",
    "DATA_ENGINEER",
    "ANALYST",
    "DATA_STEWARD",
    "PLATFORM_ADMIN",
  ])
  name    = each.key
  comment = "Functional role: ${each.key}"
}

# --- Role Hierarchy ---
# LOADER → TRANSFORMER → DATA_ENGINEER → PLATFORM_ADMIN → SYSADMIN
resource "snowflake_grant_account_role" "loader_to_transformer" {
  role_name        = snowflake_account_role.roles["LOADER"].name
  parent_role_name = snowflake_account_role.roles["TRANSFORMER"].name
}

resource "snowflake_grant_account_role" "transformer_to_engineer" {
  role_name        = snowflake_account_role.roles["TRANSFORMER"].name
  parent_role_name = snowflake_account_role.roles["DATA_ENGINEER"].name
}

resource "snowflake_grant_account_role" "engineer_to_admin" {
  role_name        = snowflake_account_role.roles["DATA_ENGINEER"].name
  parent_role_name = snowflake_account_role.roles["PLATFORM_ADMIN"].name
}

resource "snowflake_grant_account_role" "admin_to_sysadmin" {
  role_name        = snowflake_account_role.roles["PLATFORM_ADMIN"].name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "steward_to_admin" {
  role_name        = snowflake_account_role.roles["DATA_STEWARD"].name
  parent_role_name = snowflake_account_role.roles["PLATFORM_ADMIN"].name
}

resource "snowflake_grant_account_role" "analyst_to_engineer" {
  role_name        = snowflake_account_role.roles["ANALYST"].name
  parent_role_name = snowflake_account_role.roles["DATA_ENGINEER"].name
}

# --- Terraform Service Account ---
resource "snowflake_user" "terraform_svc" {
  name              = "TERRAFORM_SVC_USER"
  default_role      = "TERRAFORM_DEPLOYER"
  default_warehouse = "CI_WH"
  comment           = "Terraform IaC service account — RSA key-pair auth only"
  must_change_password = false
}

# --- Least-Privilege Terraform Deployer Role ---
resource "snowflake_account_role" "terraform_deployer" {
  name    = "TERRAFORM_DEPLOYER"
  comment = "Least-privilege role for Terraform IaC — scoped to infra management only"
}

resource "snowflake_grant_account_role" "deployer_to_sysadmin" {
  role_name        = snowflake_account_role.terraform_deployer.name
  parent_role_name = "SYSADMIN"
}

resource "snowflake_grant_account_role" "deployer_to_securityadmin" {
  role_name        = snowflake_account_role.terraform_deployer.name
  parent_role_name = "SECURITYADMIN"
}

resource "snowflake_grant_account_role" "deployer_to_svc_user" {
  role_name        = snowflake_account_role.terraform_deployer.name
  user_name        = snowflake_user.terraform_svc.name
}
