{{/*
  secure_customer_profiles.sql — Secure View: Customer Profiles
  
  PURPOSE: Masked view of customer data for external BI consumers.
           Uses Snowflake SECURE VIEW to prevent query optimization leaks.
  
  MASKING: PII columns are masked based on the querying role.
           This view inherits table-level masking policies, so no
           additional masking logic needed here.
*/}}

{{
    config(
        materialized='view',
        secure=true
    )
}}

SELECT
    CUSTOMER_SK,
    CUSTOMER_ID,

    -- PII (masked by underlying table masking policies)
    FULL_NAME,
    EMAIL,
    PHONE,
    COUNTRY_CODE,
    CITY,
    STATE,

    -- Non-PII demographics
    GENDER,
    CUSTOMER_SEGMENT,
    LOYALTY_TIER,

    -- Business classifications
    RFM_SEGMENT,
    LTV_TIER,
    CHURN_RISK,
    IS_ACTIVE,

    -- Aggregated metrics (non-PII)
    TOTAL_ORDERS,
    TOTAL_REVENUE,
    AVG_ORDER_VALUE,
    LAST_ORDER_DATE,

    -- Audit
    LAST_UPDATED_AT

FROM {{ ref('dim_customer') }}
WHERE IS_CURRENT = TRUE
