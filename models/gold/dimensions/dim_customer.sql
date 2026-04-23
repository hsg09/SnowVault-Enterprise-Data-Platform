{{/*
  dim_customer.sql — Dimension: Customer (SCD Type 2)
  
  GRAIN: One row per customer (current state). Historical versions
         available via snapshots or satellite history.
*/}}

{{
    config(
        materialized='table'
    )
}}

SELECT
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['HK_CUSTOMER']) }}
                                                    AS CUSTOMER_SK,
    HK_CUSTOMER,
    CUSTOMER_ID,

    -- Attributes
    FIRST_NAME,
    LAST_NAME,
    FIRST_NAME || ' ' || LAST_NAME                  AS FULL_NAME,
    EMAIL,
    PHONE,
    COUNTRY_CODE,
    CITY,
    STATE,
    POSTAL_CODE,
    REGISTRATION_DATE,

    -- Demographics
    DATE_OF_BIRTH,
    GENDER,
    CUSTOMER_SEGMENT,
    LOYALTY_TIER,

    -- Business classifications
    RFM_SEGMENT,
    LTV_TIER,
    CHURN_RISK,
    TOTAL_ORDERS,
    TOTAL_REVENUE,
    AVG_ORDER_VALUE,
    RECENCY_SCORE,
    FREQUENCY_SCORE,
    MONETARY_SCORE,

    -- Status flags
    CASE
        WHEN CHURN_RISK = 'HIGH' OR DAYS_SINCE_LAST_ORDER > 365 THEN FALSE
        ELSE TRUE
    END                                             AS IS_ACTIVE,

    -- Dates
    FIRST_LOADED_AT,
    LAST_ORDER_DATE,
    CONFORMED_AT                                    AS LAST_UPDATED_AT,

    -- SCD metadata (current record indicator)
    TRUE                                            AS IS_CURRENT,
    FIRST_LOADED_AT                                 AS EFFECTIVE_FROM,
    CAST('9999-12-31' AS TIMESTAMP_NTZ)             AS EFFECTIVE_TO

FROM {{ ref('conformed_customers') }}
