{{/*
  fct_orders.sql — Fact Table: Orders
  
  GRAIN: One row per order.
  SOURCE: conformed_orders (Silver layer)
  MEASURES: Total amount, net amount, discount, tax, fulfillment hours
*/}}

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='ORDER_SK',
        on_schema_change='sync_all_columns',
        cluster_by=['ORDER_DATE_KEY']
    )
}}

WITH orders AS (

    SELECT * FROM {{ ref('conformed_orders') }}

    {% if is_incremental() %}
    WHERE CONFORMED_AT > (SELECT COALESCE(MAX(UPDATED_AT), '1900-01-01') FROM {{ this }})
    {% endif %}

)

SELECT
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['ORDER_ID']) }}
                                                    AS ORDER_SK,

    -- Degenerate dimensions
    HK_ORDER,
    ORDER_ID,
    HK_CUSTOMER,

    -- Foreign keys to dimensions
    {{ dbt_utils.generate_surrogate_key(['HK_CUSTOMER']) }}
                                                    AS CUSTOMER_SK,
    TO_VARCHAR(ORDER_DATE, 'YYYYMMDD')              AS ORDER_DATE_KEY,

    -- Date dimensions
    ORDER_DATE,
    FULFILLMENT_DATE,

    -- Order attributes (degenerate)
    ORDER_STATUS,
    LIFECYCLE_STAGE,
    IS_TERMINAL,
    SHIPPING_METHOD,
    PAYMENT_METHOD,
    CURRENCY_CODE,

    -- Measures
    TOTAL_AMOUNT,
    DISCOUNT_AMOUNT,
    TAX_AMOUNT,
    NET_AMOUNT,
    HOURS_TO_FULFILLMENT,
    TOTAL_STATUS_CHANGES,

    -- SLA
    SLA_MET,

    -- Audit
    CONFORMED_AT                                    AS UPDATED_AT

FROM orders
