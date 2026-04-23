{{/*
  dim_product.sql — Dimension: Product
  
  GRAIN: One row per product (current state).
*/}}

{{
    config(
        materialized='table'
    )
}}

WITH hub AS (

    SELECT
        HK_PRODUCT,
        PRODUCT_ID,
        LOAD_DATETIME AS FIRST_LOADED_AT
    FROM {{ ref('hub_product') }}

),

details AS (

    SELECT *
    FROM {{ ref('sat_product_details') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY HK_PRODUCT ORDER BY LOAD_DATETIME DESC
    ) = 1

),

pricing AS (

    SELECT *
    FROM {{ ref('sat_product_pricing') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY HK_PRODUCT ORDER BY LOAD_DATETIME DESC
    ) = 1

)

SELECT
    -- Surrogate key
    {{ dbt_utils.generate_surrogate_key(['hub.HK_PRODUCT']) }}
                                                    AS PRODUCT_SK,
    hub.HK_PRODUCT,
    hub.PRODUCT_ID,

    -- Product attributes
    det.PRODUCT_NAME,
    det.CATEGORY,
    det.SUBCATEGORY,
    det.BRAND,
    det.WEIGHT_KG,
    det.IS_ACTIVE,
    det.CREATED_AT                                  AS PRODUCT_CREATED_AT,
    det.UPDATED_AT                                  AS PRODUCT_UPDATED_AT,

    -- Pricing
    pr.UNIT_PRICE,
    pr.COST_PRICE,
    CASE
        WHEN pr.COST_PRICE > 0
            THEN ROUND((pr.UNIT_PRICE - pr.COST_PRICE) / pr.COST_PRICE * 100, 2)
        ELSE 0
    END                                             AS MARGIN_PERCENT,

    -- Dates
    hub.FIRST_LOADED_AT,
    det.LOAD_DATETIME                               AS DETAILS_LAST_UPDATED,
    pr.LOAD_DATETIME                                AS PRICING_LAST_UPDATED,

    -- SCD metadata
    TRUE                                            AS IS_CURRENT,
    hub.FIRST_LOADED_AT                             AS EFFECTIVE_FROM,
    CAST('9999-12-31' AS TIMESTAMP_NTZ)             AS EFFECTIVE_TO

FROM hub
LEFT JOIN details det ON hub.HK_PRODUCT = det.HK_PRODUCT
LEFT JOIN pricing pr  ON hub.HK_PRODUCT = pr.HK_PRODUCT
