{{/*
  fct_order_items.sql — Fact Table: Order Line Items
  
  GRAIN: One row per order line item.
  SOURCE: staging order items + conformed orders (for enrichment)
  MEASURES: Quantity, unit price, discount, line total
*/}}

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='ORDER_ITEM_SK',
        on_schema_change='sync_all_columns'
    )
}}

WITH items AS (

    SELECT
        oi.HK_LINK_ORDER_PRODUCT,
        oi.HK_ORDER,
        oi.HK_PRODUCT,
        oi.ORDER_ITEM_ID,
        oi.ORDER_ID,
        oi.PRODUCT_ID,
        oi.QUANTITY,
        oi.UNIT_PRICE,
        oi.DISCOUNT_PERCENT,
        oi.LINE_TOTAL,
        oi.LOAD_DATETIME,
        oi.RECORD_SOURCE
    FROM {{ ref('stg_ecommerce__order_items') }} oi

    {% if is_incremental() %}
    WHERE oi.LOAD_DATETIME > (SELECT COALESCE(MAX(UPDATED_AT), '1900-01-01') FROM {{ this }})
    {% endif %}

),

orders AS (

    SELECT
        HK_ORDER,
        ORDER_DATE,
        CURRENCY_CODE
    FROM {{ ref('conformed_orders') }}

)

SELECT
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['items.ORDER_ITEM_ID']) }}
                                                    AS ORDER_ITEM_SK,

    -- Foreign keys
    {{ dbt_utils.generate_surrogate_key(['items.ORDER_ID']) }}
                                                    AS ORDER_SK,
    {{ dbt_utils.generate_surrogate_key(['items.HK_PRODUCT']) }}
                                                    AS PRODUCT_SK,
    items.HK_LINK_ORDER_PRODUCT,
    items.HK_ORDER,
    items.HK_PRODUCT,

    -- Degenerate dimensions
    items.ORDER_ITEM_ID,
    items.ORDER_ID,
    items.PRODUCT_ID,

    -- Date key
    TO_VARCHAR(ord.ORDER_DATE, 'YYYYMMDD')          AS ORDER_DATE_KEY,
    ord.ORDER_DATE,

    -- Measures
    items.QUANTITY,
    items.UNIT_PRICE,
    items.DISCOUNT_PERCENT,
    items.LINE_TOTAL,
    items.QUANTITY * items.UNIT_PRICE                AS GROSS_AMOUNT,
    items.QUANTITY * items.UNIT_PRICE * (items.DISCOUNT_PERCENT / 100)
                                                    AS DISCOUNT_AMOUNT,

    -- Currency
    ord.CURRENCY_CODE,

    -- Audit
    items.LOAD_DATETIME                             AS UPDATED_AT

FROM items
LEFT JOIN orders ord ON items.HK_ORDER = ord.HK_ORDER
