-- =============================================================================
-- 09_network_policies.sql — Network Security
-- Data Vault 2.0 Snowflake Data Platform
--
-- PURPOSE: Enforce IP allowlisting and prepare for PrivateLink connectivity.
--          Restrict access to known CIDR ranges for each environment.
--
-- EXECUTION ORDER: Run AFTER account-level setup
-- REQUIRES: ACCOUNTADMIN or SECURITYADMIN
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- 1. NETWORK POLICIES
-- =============================================================================

-- Production network policy — tightest restrictions
CREATE NETWORK POLICY IF NOT EXISTS NP_PRODUCTION
    ALLOWED_IP_LIST = (
        -- Corporate VPN CIDR ranges (replace with actual)
        '10.0.0.0/8',
        -- CI/CD runners (GitHub Actions IP ranges — update periodically)
        '140.82.112.0/20',
        -- Airflow deployment (replace with actual)
        '172.16.0.0/12'
    )
    BLOCKED_IP_LIST = (
        '0.0.0.0/0'        -- Block all by default (allowlist takes precedence)
    )
    COMMENT = 'Production — VPN, CI/CD runners, and orchestration only';

-- Development network policy — broader access for dev workflows
CREATE NETWORK POLICY IF NOT EXISTS NP_DEVELOPMENT
    ALLOWED_IP_LIST = (
        -- Corporate VPN
        '10.0.0.0/8',
        -- Developer home IPs (managed via Terraform variable)
        '0.0.0.0/0'        -- Open for dev — restrict in staging/prod
    )
    COMMENT = 'Development — broader access for development workflows';

-- Service accounts network policy
CREATE NETWORK POLICY IF NOT EXISTS NP_SERVICE_ACCOUNTS
    ALLOWED_IP_LIST = (
        -- CI/CD runners
        '140.82.112.0/20',
        -- Airflow / orchestration
        '172.16.0.0/12',
        -- AWS PrivateLink endpoints (replace with actual)
        '10.0.0.0/8'
    )
    COMMENT = 'Service accounts — CI/CD, orchestration, and PrivateLink only';

-- =============================================================================
-- 2. POLICY ASSIGNMENT
-- =============================================================================

-- Assign to service accounts (most restrictive)
ALTER USER SVC_DBT_TRANSFORMER SET NETWORK_POLICY = NP_SERVICE_ACCOUNTS;
ALTER USER SVC_AIRFLOW         SET NETWORK_POLICY = NP_SERVICE_ACCOUNTS;
ALTER USER SVC_SNOWPIPE        SET NETWORK_POLICY = NP_SERVICE_ACCOUNTS;
ALTER USER SVC_TERRAFORM       SET NETWORK_POLICY = NP_SERVICE_ACCOUNTS;

-- Account-level policy (can be set per environment via Terraform)
-- WARNING: Setting account-level policy locks out users not in ALLOWED_IP_LIST
-- ALTER ACCOUNT SET NETWORK_POLICY = NP_PRODUCTION;

-- =============================================================================
-- 3. AWS PRIVATELINK (for production)
-- =============================================================================

-- NOTE: PrivateLink requires Snowflake Business Critical Edition or higher.
-- Steps:
-- 1. Create VPC endpoint in AWS targeting Snowflake's PrivateLink service
-- 2. Authorize the endpoint in Snowflake:

-- SELECT SYSTEM$AUTHORIZE_PRIVATELINK(
--     '<aws_account_id>',
--     'com.amazonaws.vpce.<region>.<vpce-id>'
-- );

-- 3. Verify:
-- SELECT SYSTEM$GET_PRIVATELINK_CONFIG();

-- =============================================================================
-- 4. AZURE PRIVATE ENDPOINT (prepared for Phase 2)
-- =============================================================================

-- -- Follow Azure Private Endpoint setup:
-- -- 1. Create Private Endpoint in Azure Portal targeting Snowflake
-- -- 2. Approve the endpoint:
-- SELECT SYSTEM$AUTHORIZE_PRIVATELINK(
--     '<azure_subscription_id>',
--     '<resource_group>/<private_endpoint_name>'
-- );

-- =============================================================================
-- 5. VERIFICATION
-- =============================================================================

-- List all network policies
-- SHOW NETWORK POLICIES;

-- Show policy assignment per user
-- SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.POLICY_REFERENCES
-- WHERE POLICY_KIND = 'NETWORK_POLICY';
