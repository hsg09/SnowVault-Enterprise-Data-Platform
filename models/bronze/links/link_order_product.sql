{{/*
  link_order_product.sql — LINK_ORDER_PRODUCT
  Data Vault 2.0 Link: Relationship between Order, Product, and Order Item.
  
  GRAIN: One row per unique (ORDER_ID, PRODUCT_ID, ORDER_ITEM_ID) combination.
  NOTE: This is a 3-way link (dependent child key ORDER_ITEM_ID included).
*/}}

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='HK_LINK_ORDER_PRODUCT',
        on_schema_change='fail'
    )
}}

WITH staging AS (

    SELECT
        HK_LINK_ORDER_PRODUCT,
        HK_ORDER,
        HK_PRODUCT,
        ORDER_ITEM_ID,
        LOAD_DATETIME,
        RECORD_SOURCE
    FROM {{ ref('stg_ecommerce__order_items') }}

    {% if is_incremental() %}
    WHERE LOAD_DATETIME > (SELECT COALESCE(MAX(LOAD_DATETIME), '1900-01-01') FROM {{ this }})
    {% endif %}

),

deduplicated AS (

    SELECT
        HK_LINK_ORDER_PRODUCT,
        HK_ORDER,
        HK_PRODUCT,
        ORDER_ITEM_ID,
        LOAD_DATETIME,
        RECORD_SOURCE,
        ROW_NUMBER() OVER (
            PARTITION BY HK_LINK_ORDER_PRODUCT
            ORDER BY LOAD_DATETIME ASC
        ) AS ROW_NUM

    FROM staging

)

SELECT
    HK_LINK_ORDER_PRODUCT,
    HK_ORDER,
    HK_PRODUCT,
    ORDER_ITEM_ID,
    LOAD_DATETIME,
    RECORD_SOURCE

FROM deduplicated
WHERE ROW_NUM = 1
{% if is_incremental() %}
  AND HK_LINK_ORDER_PRODUCT NOT IN (SELECT HK_LINK_ORDER_PRODUCT FROM {{ this }})
{% endif %}
