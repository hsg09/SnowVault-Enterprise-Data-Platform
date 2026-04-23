{{/*
  conformed_orders.sql — Conformed Integration Layer: Orders
  
  PURPOSE: Flatten Data Vault structures into a single denormalized order
           record for Gold layer consumption. Combines hub, satellites,
           link, and business vault lifecycle.
*/}}

{{
    config(
        materialized='table'
    )
}}

WITH hub AS (

    SELECT
        HK_ORDER,
        ORDER_ID,
        LOAD_DATETIME AS FIRST_LOADED_AT
    FROM {{ ref('hub_order') }}

),

details AS (

    SELECT *
    FROM {{ ref('sat_order_details') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY HK_ORDER ORDER BY LOAD_DATETIME DESC
    ) = 1

),

financials AS (

    SELECT *
    FROM {{ ref('sat_order_financials') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY HK_ORDER ORDER BY LOAD_DATETIME DESC
    ) = 1

),

customer_link AS (

    SELECT
        HK_ORDER,
        HK_CUSTOMER
    FROM {{ ref('link_customer_order') }}

),

lifecycle AS (

    SELECT *
    FROM {{ ref('bv_order_lifecycle') }}

)

SELECT
    hub.HK_ORDER,
    hub.ORDER_ID,
    hub.FIRST_LOADED_AT,

    -- Customer relationship
    cl.HK_CUSTOMER,

    -- Order details
    det.ORDER_DATE,
    det.ORDER_STATUS,
    det.SHIPPING_METHOD,
    det.SHIPPING_ADDRESS,
    det.BILLING_ADDRESS,
    det.FULFILLMENT_DATE,

    -- Financials
    fin.TOTAL_AMOUNT,
    fin.CURRENCY_CODE,
    fin.PAYMENT_METHOD,
    fin.DISCOUNT_AMOUNT,
    fin.TAX_AMOUNT,

    -- Lifecycle (from Business Vault)
    lc.NET_AMOUNT,
    lc.LIFECYCLE_STAGE,
    lc.IS_TERMINAL,
    lc.SLA_MET,
    lc.HOURS_TO_FULFILLMENT,
    lc.DAYS_SINCE_ORDER,
    lc.TOTAL_STATUS_CHANGES,

    -- Audit
    det.LOAD_DATETIME   AS DETAILS_LAST_UPDATED,
    fin.LOAD_DATETIME   AS FINANCIALS_LAST_UPDATED,
    CURRENT_TIMESTAMP() AS CONFORMED_AT

FROM hub
LEFT JOIN details det       ON hub.HK_ORDER = det.HK_ORDER
LEFT JOIN financials fin    ON hub.HK_ORDER = fin.HK_ORDER
LEFT JOIN customer_link cl  ON hub.HK_ORDER = cl.HK_ORDER
LEFT JOIN lifecycle lc      ON hub.HK_ORDER = lc.HK_ORDER
