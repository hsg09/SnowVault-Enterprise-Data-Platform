{{/*
  stg_ecommerce__products.sql — Pre-Vault Staging: Products
  
  PURPOSE: Transform raw product data into Data Vault-ready format.
           - Generate SHA-256 hash keys (HK_PRODUCT)
           - Generate hash diffs for satellite change detection
           - Split into details vs pricing satellites (different change rates)
*/}}

WITH source AS (

    SELECT * FROM {{ source('raw_ecommerce', 'RAW_PRODUCTS') }}

),

renamed AS (

    SELECT
        -- ===== Business Key =====
        TRIM(UPPER(PRODUCT_ID))                         AS PRODUCT_ID,

        -- ===== Descriptive Attributes (Satellite: Product Details) =====
        TRIM(PRODUCT_NAME)                              AS PRODUCT_NAME,
        TRIM(UPPER(CATEGORY))                           AS CATEGORY,
        TRIM(UPPER(SUBCATEGORY))                        AS SUBCATEGORY,
        TRIM(UPPER(BRAND))                              AS BRAND,
        COALESCE(WEIGHT_KG, 0)                          AS WEIGHT_KG,
        COALESCE(IS_ACTIVE, TRUE)                       AS IS_ACTIVE,
        CREATED_AT,
        UPDATED_AT,

        -- ===== Descriptive Attributes (Satellite: Product Pricing) =====
        COALESCE(UNIT_PRICE, 0)                         AS UNIT_PRICE,
        COALESCE(COST_PRICE, 0)                         AS COST_PRICE,

        -- ===== Metadata =====
        _LOADED_AT,
        _FILE_NAME,
        _FILE_ROW_NUMBER

    FROM source

),

hashed AS (

    SELECT
        *,

        -- ===== Hash Key (Hub) =====
        {{ dbt_utils.generate_surrogate_key(['PRODUCT_ID']) }}
                                                        AS HK_PRODUCT,

        -- ===== Hash Diff (Satellite: Product Details) =====
        {{ dbt_utils.generate_surrogate_key([
            'PRODUCT_NAME',
            'CATEGORY',
            'SUBCATEGORY',
            'BRAND',
            'WEIGHT_KG',
            'IS_ACTIVE'
        ]) }}                                           AS HD_PRODUCT_DETAILS,

        -- ===== Hash Diff (Satellite: Product Pricing) =====
        {{ dbt_utils.generate_surrogate_key([
            'UNIT_PRICE',
            'COST_PRICE'
        ]) }}                                           AS HD_PRODUCT_PRICING,

        -- ===== Load Metadata =====
        _LOADED_AT                                      AS LOAD_DATETIME,
        'ECOMMERCE.RAW_PRODUCTS'                        AS RECORD_SOURCE,
        CURRENT_DATE()                                  AS APPLIED_DATE

    FROM renamed

)

SELECT * FROM hashed
