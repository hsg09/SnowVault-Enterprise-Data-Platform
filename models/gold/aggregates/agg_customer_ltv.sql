{{/*
  agg_customer_ltv.sql — Aggregate: Customer Lifetime Value
  
  GRAIN: One row per customer.
  MEASURES: LTV, order frequency, tenure, predicted value.
*/}}

{{
    config(
        materialized='table'
    )
}}

WITH customer AS (

    SELECT * FROM {{ ref('dim_customer') }}

),

order_history AS (

    SELECT
        HK_CUSTOMER,
        COUNT(*)                                    AS ORDER_COUNT,
        SUM(NET_AMOUNT)                             AS TOTAL_SPEND,
        AVG(NET_AMOUNT)                             AS AVG_ORDER_VALUE,
        MIN(ORDER_DATE)                             AS FIRST_ORDER_DATE,
        MAX(ORDER_DATE)                             AS LAST_ORDER_DATE,
        DATEDIFF('month',
            MIN(ORDER_DATE),
            MAX(ORDER_DATE)
        )                                           AS CUSTOMER_TENURE_MONTHS,
        DATEDIFF('day',
            MAX(ORDER_DATE),
            CURRENT_DATE()
        )                                           AS DAYS_SINCE_LAST_ORDER
    FROM {{ ref('fct_orders') }}
    WHERE ORDER_STATUS NOT IN ('CANCELLED', 'REFUNDED')
    GROUP BY HK_CUSTOMER

)

SELECT
    c.CUSTOMER_SK,
    c.HK_CUSTOMER,
    c.CUSTOMER_ID,
    c.FULL_NAME,
    c.COUNTRY_CODE,
    c.CUSTOMER_SEGMENT,
    c.LOYALTY_TIER,
    c.RFM_SEGMENT,

    -- LTV measures
    COALESCE(oh.ORDER_COUNT, 0)                     AS LIFETIME_ORDER_COUNT,
    COALESCE(oh.TOTAL_SPEND, 0)                     AS LIFETIME_SPEND,
    COALESCE(oh.AVG_ORDER_VALUE, 0)                 AS LIFETIME_AOV,
    oh.FIRST_ORDER_DATE,
    oh.LAST_ORDER_DATE,
    COALESCE(oh.CUSTOMER_TENURE_MONTHS, 0)          AS TENURE_MONTHS,
    COALESCE(oh.DAYS_SINCE_LAST_ORDER, 9999)        AS DAYS_SINCE_LAST_ORDER,

    -- Purchase frequency (orders per month)
    CASE
        WHEN COALESCE(oh.CUSTOMER_TENURE_MONTHS, 0) > 0
            THEN ROUND(oh.ORDER_COUNT / oh.CUSTOMER_TENURE_MONTHS, 2)
        ELSE 0
    END                                             AS ORDERS_PER_MONTH,

    -- Monthly spend rate
    CASE
        WHEN COALESCE(oh.CUSTOMER_TENURE_MONTHS, 0) > 0
            THEN ROUND(oh.TOTAL_SPEND / oh.CUSTOMER_TENURE_MONTHS, 2)
        ELSE 0
    END                                             AS MONTHLY_SPEND_RATE,

    -- Predicted 12-month value (simple projection)
    CASE
        WHEN COALESCE(oh.CUSTOMER_TENURE_MONTHS, 0) > 0
            THEN ROUND(oh.TOTAL_SPEND / oh.CUSTOMER_TENURE_MONTHS * 12, 2)
        ELSE 0
    END                                             AS PREDICTED_12M_VALUE,

    -- LTV classification
    c.LTV_TIER,
    c.CHURN_RISK,

    -- Percentile rank
    PERCENT_RANK() OVER (ORDER BY COALESCE(oh.TOTAL_SPEND, 0))
                                                    AS SPEND_PERCENTILE,

    CURRENT_TIMESTAMP()                             AS AGGREGATED_AT

FROM customer c
LEFT JOIN order_history oh ON c.HK_CUSTOMER = oh.HK_CUSTOMER
