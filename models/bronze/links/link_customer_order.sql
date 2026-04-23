{{/*
  link_customer_order.sql — LINK_CUSTOMER_ORDER
  Data Vault 2.0 Link: Relationship between Customer and Order.
  
  GRAIN: One row per unique (CUSTOMER_ID, ORDER_ID) pair ever observed.
  PATTERN: Insert-only. Composite hash key from FK hash keys.
*/}}

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='HK_LINK_CUSTOMER_ORDER',
        on_schema_change='fail'
    )
}}

WITH staging AS (

    SELECT
        HK_LINK_CUSTOMER_ORDER,
        HK_CUSTOMER,
        HK_ORDER,
        LOAD_DATETIME,
        RECORD_SOURCE
    FROM {{ ref('stg_ecommerce__orders') }}

    {% if is_incremental() %}
    WHERE LOAD_DATETIME > (SELECT COALESCE(MAX(LOAD_DATETIME), '1900-01-01') FROM {{ this }})
    {% endif %}

),

deduplicated AS (

    SELECT
        HK_LINK_CUSTOMER_ORDER,
        HK_CUSTOMER,
        HK_ORDER,
        LOAD_DATETIME,
        RECORD_SOURCE,
        ROW_NUMBER() OVER (
            PARTITION BY HK_LINK_CUSTOMER_ORDER
            ORDER BY LOAD_DATETIME ASC
        ) AS ROW_NUM

    FROM staging

)

SELECT
    HK_LINK_CUSTOMER_ORDER,
    HK_CUSTOMER,
    HK_ORDER,
    LOAD_DATETIME,
    RECORD_SOURCE

FROM deduplicated
WHERE ROW_NUM = 1
{% if is_incremental() %}
  AND HK_LINK_CUSTOMER_ORDER NOT IN (SELECT HK_LINK_CUSTOMER_ORDER FROM {{ this }})
{% endif %}
