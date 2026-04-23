# Dev environment overrides
environment            = "dev"
snowflake_organization = "YOUR_ORG"
snowflake_account      = "YOUR_DEV_ACCOUNT"
snowflake_user         = "TERRAFORM_SVC_USER"
aws_s3_role_arn        = "arn:aws:iam::role/snowflake-data-lake-access"
azure_tenant_id        = ""
gcs_allowed_locations  = []
secondary_accounts     = []
replication_schedule   = "USING CRON 0 */4 * * * UTC"
allowed_ip_list        = ["0.0.0.0/0"]
credit_quota_monthly   = 100
