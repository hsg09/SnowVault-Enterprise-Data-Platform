{{/*
  bv_customer_classification.sql — Business Vault: Customer Classification
  
  PURPOSE: Derive business-rule-driven customer classifications:
           - RFM segmentation (Recency, Frequency, Monetary)
           - Customer lifetime value tier
           - Churn risk score
  
  PATTERN: Business Vault — applies business logic on top of Raw Vault data.
           Not source-system-driven; these are enterprise-defined rules.
*/}}

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='HK_CUSTOMER',
        on_schema_change='sync_all_columns'
    )
}}

WITH customer_hub AS (

    SELECT
        HK_CUSTOMER,
        CUSTOMER_ID,
        LOAD_DATETIME AS FIRST_SEEN_DATE
    FROM {{ ref('hub_customer') }}

),

customer_details AS (

    SELECT
        HK_CUSTOMER,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        COUNTRY_CODE,
        REGISTRATION_DATE
    FROM {{ ref('sat_customer_details') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY HK_CUSTOMER ORDER BY LOAD_DATETIME DESC
    ) = 1

),

customer_demographics AS (

    SELECT
        HK_CUSTOMER,
        CUSTOMER_SEGMENT,
        LOYALTY_TIER
    FROM {{ ref('sat_customer_demographics') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY HK_CUSTOMER ORDER BY LOAD_DATETIME DESC
    ) = 1

),

-- Aggregate order history for RFM
order_history AS (

    SELECT
        lco.HK_CUSTOMER,
        COUNT(DISTINCT lco.HK_ORDER)                    AS TOTAL_ORDERS,
        MAX(sod.ORDER_DATE)                             AS LAST_ORDER_DATE,
        MIN(sod.ORDER_DATE)                             AS FIRST_ORDER_DATE,
        SUM(sof.TOTAL_AMOUNT)                           AS TOTAL_REVENUE,
        AVG(sof.TOTAL_AMOUNT)                           AS AVG_ORDER_VALUE,
        DATEDIFF('day', MAX(sod.ORDER_DATE), CURRENT_DATE()) AS DAYS_SINCE_LAST_ORDER
    FROM {{ ref('link_customer_order') }} lco
    INNER JOIN {{ ref('sat_order_details') }} sod
        ON lco.HK_ORDER = sod.HK_ORDER
        AND sod.ORDER_STATUS NOT IN ('CANCELLED', 'REFUNDED')
    INNER JOIN {{ ref('sat_order_financials') }} sof
        ON lco.HK_ORDER = sof.HK_ORDER
    -- Get latest satellite records
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY lco.HK_ORDER, sod.HK_ORDER ORDER BY sod.LOAD_DATETIME DESC
    ) = 1
    GROUP BY lco.HK_CUSTOMER

),

-- RFM scoring
rfm_scored AS (

    SELECT
        HK_CUSTOMER,
        TOTAL_ORDERS,
        LAST_ORDER_DATE,
        FIRST_ORDER_DATE,
        TOTAL_REVENUE,
        AVG_ORDER_VALUE,
        DAYS_SINCE_LAST_ORDER,

        -- Recency score (1=worst, 5=best)
        NTILE(5) OVER (ORDER BY DAYS_SINCE_LAST_ORDER DESC)    AS RECENCY_SCORE,
        -- Frequency score
        NTILE(5) OVER (ORDER BY TOTAL_ORDERS ASC)              AS FREQUENCY_SCORE,
        -- Monetary score
        NTILE(5) OVER (ORDER BY TOTAL_REVENUE ASC)             AS MONETARY_SCORE

    FROM order_history

),

classified AS (

    SELECT
        hub.HK_CUSTOMER,
        hub.CUSTOMER_ID,
        det.FIRST_NAME,
        det.LAST_NAME,
        det.COUNTRY_CODE,
        det.REGISTRATION_DATE,
        dem.CUSTOMER_SEGMENT,
        dem.LOYALTY_TIER,

        -- Order metrics
        COALESCE(rfm.TOTAL_ORDERS, 0)                  AS TOTAL_ORDERS,
        rfm.LAST_ORDER_DATE,
        rfm.FIRST_ORDER_DATE,
        COALESCE(rfm.TOTAL_REVENUE, 0)                 AS TOTAL_REVENUE,
        COALESCE(rfm.AVG_ORDER_VALUE, 0)                AS AVG_ORDER_VALUE,
        COALESCE(rfm.DAYS_SINCE_LAST_ORDER, 9999)      AS DAYS_SINCE_LAST_ORDER,

        -- RFM scores
        COALESCE(rfm.RECENCY_SCORE, 1)                 AS RECENCY_SCORE,
        COALESCE(rfm.FREQUENCY_SCORE, 1)               AS FREQUENCY_SCORE,
        COALESCE(rfm.MONETARY_SCORE, 1)                 AS MONETARY_SCORE,

        -- Combined RFM segment
        COALESCE(rfm.RECENCY_SCORE, 1) +
        COALESCE(rfm.FREQUENCY_SCORE, 1) +
        COALESCE(rfm.MONETARY_SCORE, 1)                 AS RFM_TOTAL_SCORE,

        -- Customer LTV tier
        CASE
            WHEN COALESCE(rfm.TOTAL_REVENUE, 0) >= 10000 THEN 'PLATINUM'
            WHEN COALESCE(rfm.TOTAL_REVENUE, 0) >= 5000  THEN 'GOLD'
            WHEN COALESCE(rfm.TOTAL_REVENUE, 0) >= 1000  THEN 'SILVER'
            WHEN COALESCE(rfm.TOTAL_REVENUE, 0) > 0      THEN 'BRONZE'
            ELSE 'NEW'
        END                                             AS LTV_TIER,

        -- Churn risk
        CASE
            WHEN COALESCE(rfm.DAYS_SINCE_LAST_ORDER, 9999) > 365 THEN 'HIGH'
            WHEN COALESCE(rfm.DAYS_SINCE_LAST_ORDER, 9999) > 180 THEN 'MEDIUM'
            WHEN COALESCE(rfm.DAYS_SINCE_LAST_ORDER, 9999) > 90  THEN 'LOW'
            ELSE 'ACTIVE'
        END                                             AS CHURN_RISK,

        -- RFM segment label
        CASE
            WHEN COALESCE(rfm.RECENCY_SCORE, 1) >= 4 AND COALESCE(rfm.FREQUENCY_SCORE, 1) >= 4
                THEN 'CHAMPION'
            WHEN COALESCE(rfm.RECENCY_SCORE, 1) >= 4 AND COALESCE(rfm.FREQUENCY_SCORE, 1) >= 2
                THEN 'LOYAL'
            WHEN COALESCE(rfm.RECENCY_SCORE, 1) >= 3 AND COALESCE(rfm.MONETARY_SCORE, 1) >= 4
                THEN 'BIG_SPENDER'
            WHEN COALESCE(rfm.RECENCY_SCORE, 1) >= 4 AND COALESCE(rfm.FREQUENCY_SCORE, 1) = 1
                THEN 'NEW_CUSTOMER'
            WHEN COALESCE(rfm.RECENCY_SCORE, 1) <= 2 AND COALESCE(rfm.FREQUENCY_SCORE, 1) >= 3
                THEN 'AT_RISK'
            WHEN COALESCE(rfm.RECENCY_SCORE, 1) <= 2 AND COALESCE(rfm.FREQUENCY_SCORE, 1) <= 2
                THEN 'LOST'
            ELSE 'REGULAR'
        END                                             AS RFM_SEGMENT,

        CURRENT_TIMESTAMP()                             AS CLASSIFIED_AT

    FROM customer_hub hub
    LEFT JOIN customer_details det ON hub.HK_CUSTOMER = det.HK_CUSTOMER
    LEFT JOIN customer_demographics dem ON hub.HK_CUSTOMER = dem.HK_CUSTOMER
    LEFT JOIN rfm_scored rfm ON hub.HK_CUSTOMER = rfm.HK_CUSTOMER

)

SELECT * FROM classified
