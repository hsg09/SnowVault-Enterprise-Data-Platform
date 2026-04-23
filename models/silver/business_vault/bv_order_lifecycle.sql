{{/*
  bv_order_lifecycle.sql — Business Vault: Order Lifecycle
  
  PURPOSE: Derive order lifecycle stage transitions and SLA tracking.
           Calculates time-in-stage, fulfilment SLA compliance, and
           order completion rates.
*/}}

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='HK_ORDER',
        on_schema_change='sync_all_columns'
    )
}}

WITH order_hub AS (

    SELECT
        HK_ORDER,
        ORDER_ID
    FROM {{ ref('hub_order') }}

),

-- All status changes for each order (from satellite history)
order_status_history AS (

    SELECT
        HK_ORDER,
        ORDER_DATE,
        ORDER_STATUS,
        FULFILLMENT_DATE,
        SHIPPING_METHOD,
        LOAD_DATETIME,
        RECORD_SOURCE,
        LAG(ORDER_STATUS) OVER (
            PARTITION BY HK_ORDER ORDER BY LOAD_DATETIME
        )                                               AS PREVIOUS_STATUS,
        LAG(LOAD_DATETIME) OVER (
            PARTITION BY HK_ORDER ORDER BY LOAD_DATETIME
        )                                               AS PREVIOUS_STATUS_TS
    FROM {{ ref('sat_order_details') }}

),

-- Get latest state per order
current_order AS (

    SELECT *
    FROM order_status_history
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY HK_ORDER ORDER BY LOAD_DATETIME DESC
    ) = 1

),

order_financials AS (

    SELECT
        HK_ORDER,
        TOTAL_AMOUNT,
        CURRENCY_CODE,
        PAYMENT_METHOD,
        DISCOUNT_AMOUNT,
        TAX_AMOUNT
    FROM {{ ref('sat_order_financials') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY HK_ORDER ORDER BY LOAD_DATETIME DESC
    ) = 1

),

-- Count status transitions
status_transitions AS (

    SELECT
        HK_ORDER,
        COUNT(*)                                        AS TOTAL_STATUS_CHANGES,
        MIN(LOAD_DATETIME)                              AS FIRST_STATUS_TS,
        MAX(LOAD_DATETIME)                              AS LAST_STATUS_TS
    FROM order_status_history
    GROUP BY HK_ORDER

),

lifecycle AS (

    SELECT
        hub.HK_ORDER,
        hub.ORDER_ID,

        -- Current state
        cur.ORDER_DATE,
        cur.ORDER_STATUS                                AS CURRENT_STATUS,
        cur.FULFILLMENT_DATE,
        cur.SHIPPING_METHOD,

        -- Financials
        fin.TOTAL_AMOUNT,
        fin.CURRENCY_CODE,
        fin.PAYMENT_METHOD,
        fin.DISCOUNT_AMOUNT,
        fin.TAX_AMOUNT,
        fin.TOTAL_AMOUNT - fin.DISCOUNT_AMOUNT + fin.TAX_AMOUNT AS NET_AMOUNT,

        -- Lifecycle metrics
        trans.TOTAL_STATUS_CHANGES,

        -- Time calculations
        DATEDIFF('hour', cur.ORDER_DATE, cur.FULFILLMENT_DATE)     AS HOURS_TO_FULFILLMENT,
        DATEDIFF('day', cur.ORDER_DATE, CURRENT_TIMESTAMP())       AS DAYS_SINCE_ORDER,

        -- SLA compliance (example: 72h for standard, 24h for express)
        CASE
            WHEN cur.ORDER_STATUS IN ('DELIVERED', 'SHIPPED')
                AND cur.SHIPPING_METHOD = 'EXPRESS'
                AND DATEDIFF('hour', cur.ORDER_DATE, COALESCE(cur.FULFILLMENT_DATE, CURRENT_TIMESTAMP())) <= 24
                THEN TRUE
            WHEN cur.ORDER_STATUS IN ('DELIVERED', 'SHIPPED')
                AND cur.SHIPPING_METHOD = 'STANDARD'
                AND DATEDIFF('hour', cur.ORDER_DATE, COALESCE(cur.FULFILLMENT_DATE, CURRENT_TIMESTAMP())) <= 72
                THEN TRUE
            WHEN cur.ORDER_STATUS IN ('PENDING', 'CONFIRMED')
                THEN NULL   -- SLA not yet applicable
            ELSE FALSE
        END                                             AS SLA_MET,

        -- Order lifecycle stage
        CASE
            WHEN cur.ORDER_STATUS = 'DELIVERED'  THEN 'COMPLETE'
            WHEN cur.ORDER_STATUS = 'SHIPPED'    THEN 'IN_TRANSIT'
            WHEN cur.ORDER_STATUS = 'CONFIRMED'  THEN 'PROCESSING'
            WHEN cur.ORDER_STATUS = 'PENDING'    THEN 'AWAITING_CONFIRMATION'
            WHEN cur.ORDER_STATUS = 'CANCELLED'  THEN 'CANCELLED'
            WHEN cur.ORDER_STATUS = 'RETURNED'   THEN 'RETURNED'
            WHEN cur.ORDER_STATUS = 'REFUNDED'   THEN 'REFUNDED'
            ELSE 'UNKNOWN'
        END                                             AS LIFECYCLE_STAGE,

        -- Is terminal state?
        cur.ORDER_STATUS IN ('DELIVERED', 'CANCELLED', 'RETURNED', 'REFUNDED')
                                                        AS IS_TERMINAL,

        CURRENT_TIMESTAMP()                             AS COMPUTED_AT

    FROM order_hub hub
    INNER JOIN current_order cur ON hub.HK_ORDER = cur.HK_ORDER
    LEFT JOIN order_financials fin ON hub.HK_ORDER = fin.HK_ORDER
    LEFT JOIN status_transitions trans ON hub.HK_ORDER = trans.HK_ORDER

)

SELECT * FROM lifecycle
