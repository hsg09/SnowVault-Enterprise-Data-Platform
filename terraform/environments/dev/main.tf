# =============================================================================
# Terraform — Snowflake Data Platform Infrastructure
#
# Root module: Orchestrates per-environment deployments.
# Uses snowflakedb/snowflake provider with RSA key-pair authentication
# (as mandated by blueprint — no password auth for IaC).
# =============================================================================

terraform {
  required_version = ">= 1.9.0"

  required_providers {
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 0.96.0"
    }
  }

  backend "s3" {
    # Override per environment via -backend-config
    bucket         = "your-company-terraform-state"
    key            = "data-vault-platform/terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "terraform-locks"
  }
}

# =============================================================================
# Provider — RSA Key-Pair Authentication (no passwords)
# =============================================================================

provider "snowflake" {
  organization_name = var.snowflake_organization
  account_name      = var.snowflake_account
  user              = var.snowflake_user
  private_key       = var.snowflake_private_key
  role              = var.snowflake_role  # Least-privilege: TERRAFORM_DEPLOYER
}

# =============================================================================
# Modules
# =============================================================================

module "databases" {
  source      = "../modules/databases"
  environment = var.environment
}

module "warehouses" {
  source      = "../modules/warehouses"
  environment = var.environment
}

module "rbac" {
  source      = "../modules/rbac"
  environment = var.environment
  depends_on  = [module.databases, module.warehouses]
}

module "storage_integrations" {
  source             = "../modules/storage_integrations"
  environment        = var.environment
  aws_s3_role_arn    = var.aws_s3_role_arn
  azure_tenant_id    = var.azure_tenant_id
  gcs_allowed_locations = var.gcs_allowed_locations
  depends_on         = [module.databases]
}

module "network_policies" {
  source      = "../modules/network_policies"
  environment = var.environment
  allowed_ips = var.allowed_ip_list
}

module "replication" {
  source              = "../modules/replication"
  environment         = var.environment
  secondary_accounts  = var.secondary_accounts
  replication_schedule = var.replication_schedule
  depends_on          = [module.databases, module.rbac]
}

module "resource_monitors" {
  source      = "../modules/resource_monitors"
  environment = var.environment
  credit_quota_monthly = var.credit_quota_monthly
  depends_on  = [module.warehouses]
}

module "governance" {
  source      = "../modules/governance"
  environment = var.environment
  depends_on  = [module.databases, module.rbac]
}
