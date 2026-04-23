# Production environment overrides — Multi-Region Multi-Cloud
environment            = "prod"
snowflake_organization = "YOUR_ORG"
snowflake_account      = "YOUR_PROD_ACCOUNT_US_EAST_1"
snowflake_user         = "TERRAFORM_SVC_USER"
snowflake_role         = "TERRAFORM_DEPLOYER"

# --- Multi-Cloud Storage Integrations ---
aws_s3_role_arn       = "arn:aws:iam::role/snowflake-data-lake-prod"
azure_tenant_id       = "YOUR_AZURE_TENANT_ID"
gcs_allowed_locations = [
  "gcs://your-gcs-data-lake-prod/",
  "gcs://your-gcs-data-lake-prod-eu/"
]

# --- Multi-Region Replication (5-7 accounts) ---
secondary_accounts = [
  "YOUR_ORG.ACCOUNT_AWS_EU_WEST_1",       # AWS eu-west-1
  "YOUR_ORG.ACCOUNT_AZURE_WEST_EUROPE",   # Azure westeurope
  "YOUR_ORG.ACCOUNT_AZURE_SE_ASIA",       # Azure southeastasia
  "YOUR_ORG.ACCOUNT_GCP_US_CENTRAL1",     # GCP us-central1
  "YOUR_ORG.ACCOUNT_GCP_ASIA_SE1"         # GCP asia-southeast1
]
replication_schedule = "USING CRON 0 */1 * * * UTC"

# --- Network Security (production — strictly restricted) ---
allowed_ip_list = [
  "10.0.0.0/8",          # Internal VPC
  "172.16.0.0/12",       # Corporate VPN
  "203.0.113.0/24"       # Office egress IPs
]

credit_quota_monthly = 5000
