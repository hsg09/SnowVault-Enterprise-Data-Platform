{{/*
  hub_product.sql — HUB_PRODUCT
  Data Vault 2.0 Hub: Unique business keys for products.
  
  GRAIN: One row per unique PRODUCT_ID (ever observed).
  PATTERN: Insert-only, no updates.
*/}}

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='HK_PRODUCT',
        on_schema_change='fail'
    )
}}

WITH staging AS (

    SELECT
        HK_PRODUCT,
        PRODUCT_ID,
        LOAD_DATETIME,
        RECORD_SOURCE
    FROM {{ ref('stg_ecommerce__products') }}

    {% if is_incremental() %}
    WHERE LOAD_DATETIME > (SELECT COALESCE(MAX(LOAD_DATETIME), '1900-01-01') FROM {{ this }})
    {% endif %}

),

deduplicated AS (

    SELECT
        HK_PRODUCT,
        PRODUCT_ID,
        LOAD_DATETIME,
        RECORD_SOURCE,
        ROW_NUMBER() OVER (
            PARTITION BY HK_PRODUCT
            ORDER BY LOAD_DATETIME ASC
        ) AS ROW_NUM

    FROM staging

)

SELECT
    HK_PRODUCT,
    PRODUCT_ID,
    LOAD_DATETIME,
    RECORD_SOURCE

FROM deduplicated
WHERE ROW_NUM = 1
{% if is_incremental() %}
  AND HK_PRODUCT NOT IN (SELECT HK_PRODUCT FROM {{ this }})
{% endif %}
