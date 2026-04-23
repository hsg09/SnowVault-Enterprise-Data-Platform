{{/*
  sat_product_pricing.sql — SAT_PRODUCT_PRICING
  Data Vault 2.0 Satellite: Product pricing attributes.
  
  GRAIN: One row per (HK_PRODUCT, LOAD_DATETIME).
  PARENT: HUB_PRODUCT
  
  RATIONALE: Pricing changes more frequently than product details (name, brand).
             Separating into its own satellite avoids rewriting large string columns
             on every price update.
*/}}

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key=['HK_PRODUCT', 'LOAD_DATETIME'],
        on_schema_change='fail'
    )
}}

WITH staging AS (

    SELECT
        HK_PRODUCT,
        HD_PRODUCT_PRICING          AS HASH_DIFF,
        UNIT_PRICE,
        COST_PRICE,
        LOAD_DATETIME,
        RECORD_SOURCE
    FROM {{ ref('stg_ecommerce__products') }}

),

{% if is_incremental() %}
latest_records AS (

    SELECT
        HK_PRODUCT,
        HASH_DIFF
    FROM (
        SELECT
            HK_PRODUCT,
            HASH_DIFF,
            ROW_NUMBER() OVER (
                PARTITION BY HK_PRODUCT
                ORDER BY LOAD_DATETIME DESC
            ) AS ROW_NUM
        FROM {{ this }}
    )
    WHERE ROW_NUM = 1

),
{% endif %}

new_records AS (

    SELECT
        stg.HK_PRODUCT,
        stg.HASH_DIFF,
        stg.UNIT_PRICE,
        stg.COST_PRICE,
        stg.LOAD_DATETIME,
        stg.RECORD_SOURCE

    FROM staging stg

    {% if is_incremental() %}
    LEFT JOIN latest_records lr
        ON stg.HK_PRODUCT = lr.HK_PRODUCT

    WHERE lr.HK_PRODUCT IS NULL
       OR stg.HASH_DIFF != lr.HASH_DIFF
    {% endif %}

)

SELECT * FROM new_records
