{{/*
  stg_ecommerce__order_items.sql — Pre-Vault Staging: Order Items
  
  PURPOSE: Transform raw order item data into Data Vault-ready format.
           - Generate hash keys for HK_ORDER, HK_PRODUCT
           - Generate composite hash key for LINK_ORDER_PRODUCT
           - Compute derived line totals
*/}}

WITH source AS (

    SELECT * FROM {{ source('raw_ecommerce', 'RAW_ORDER_ITEMS') }}

),

renamed AS (

    SELECT
        -- ===== Business Keys =====
        TRIM(UPPER(ORDER_ITEM_ID))                      AS ORDER_ITEM_ID,
        TRIM(UPPER(ORDER_ID))                           AS ORDER_ID,
        TRIM(UPPER(PRODUCT_ID))                         AS PRODUCT_ID,

        -- ===== Measures =====
        QUANTITY,
        UNIT_PRICE,
        COALESCE(DISCOUNT_PERCENT, 0)                   AS DISCOUNT_PERCENT,
        COALESCE(LINE_TOTAL,
            QUANTITY * UNIT_PRICE * (1 - COALESCE(DISCOUNT_PERCENT, 0) / 100)
        )                                               AS LINE_TOTAL,

        -- ===== Metadata =====
        _LOADED_AT,
        _FILE_NAME,
        _FILE_ROW_NUMBER

    FROM source

),

hashed AS (

    SELECT
        *,

        -- ===== Hash Keys (Hubs) =====
        {{ dbt_utils.generate_surrogate_key(['ORDER_ID']) }}
                                                        AS HK_ORDER,
        {{ dbt_utils.generate_surrogate_key(['PRODUCT_ID']) }}
                                                        AS HK_PRODUCT,

        -- ===== Hash Key (Link: Order ↔ Product) =====
        {{ dbt_utils.generate_surrogate_key(['ORDER_ID', 'PRODUCT_ID', 'ORDER_ITEM_ID']) }}
                                                        AS HK_LINK_ORDER_PRODUCT,

        -- ===== Hash Diff (for satellite on line-item details) =====
        {{ dbt_utils.generate_surrogate_key([
            'QUANTITY',
            'UNIT_PRICE',
            'DISCOUNT_PERCENT',
            'LINE_TOTAL'
        ]) }}                                           AS HD_ORDER_ITEM_DETAILS,

        -- ===== Load Metadata =====
        _LOADED_AT                                      AS LOAD_DATETIME,
        'ECOMMERCE.RAW_ORDER_ITEMS'                     AS RECORD_SOURCE,
        CURRENT_DATE()                                  AS APPLIED_DATE

    FROM renamed

)

SELECT * FROM hashed
