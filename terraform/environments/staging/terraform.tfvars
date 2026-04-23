# Staging environment overrides
environment            = "staging"
snowflake_organization = "YOUR_ORG"
snowflake_account      = "YOUR_STAGING_ACCOUNT"
snowflake_user         = "TERRAFORM_SVC_USER"
snowflake_role         = "TERRAFORM_DEPLOYER"
aws_s3_role_arn        = "arn:aws:iam::role/snowflake-data-lake-staging"
azure_tenant_id        = ""
gcs_allowed_locations  = []
secondary_accounts     = []
replication_schedule   = "USING CRON 0 */2 * * * UTC"
allowed_ip_list        = ["10.0.0.0/8", "172.16.0.0/12"]  # Internal only
credit_quota_monthly   = 500
