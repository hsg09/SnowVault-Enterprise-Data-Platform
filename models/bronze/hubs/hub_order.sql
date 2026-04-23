{{/*
  hub_order.sql — HUB_ORDER
  Data Vault 2.0 Hub: Unique business keys for orders.
  
  GRAIN: One row per unique ORDER_ID (ever observed).
  PATTERN: Insert-only, no updates.
*/}}

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='HK_ORDER',
        on_schema_change='fail'
    )
}}

WITH staging AS (

    SELECT
        HK_ORDER,
        ORDER_ID,
        LOAD_DATETIME,
        RECORD_SOURCE
    FROM {{ ref('stg_ecommerce__orders') }}

    {% if is_incremental() %}
    WHERE LOAD_DATETIME > (SELECT COALESCE(MAX(LOAD_DATETIME), '1900-01-01') FROM {{ this }})
    {% endif %}

),

deduplicated AS (

    SELECT
        HK_ORDER,
        ORDER_ID,
        LOAD_DATETIME,
        RECORD_SOURCE,
        ROW_NUMBER() OVER (
            PARTITION BY HK_ORDER
            ORDER BY LOAD_DATETIME ASC
        ) AS ROW_NUM

    FROM staging

)

SELECT
    HK_ORDER,
    ORDER_ID,
    LOAD_DATETIME,
    RECORD_SOURCE

FROM deduplicated
WHERE ROW_NUM = 1
{% if is_incremental() %}
  AND HK_ORDER NOT IN (SELECT HK_ORDER FROM {{ this }})
{% endif %}
