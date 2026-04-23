{{/*
  stg_ecommerce__orders.sql — Pre-Vault Staging: Orders
  
  PURPOSE: Transform raw order data into Data Vault-ready format.
           - Generate SHA-256 hash keys (HK_ORDER, HK_CUSTOMER)
           - Generate composite hash key for LINK_CUSTOMER_ORDER
           - Generate hash diffs for satellite change detection
           - Add record source + load datetime metadata
*/}}

WITH source AS (

    SELECT * FROM {{ source('raw_ecommerce', 'RAW_ORDERS') }}

),

renamed AS (

    SELECT
        -- ===== Business Keys =====
        TRIM(UPPER(ORDER_ID))                           AS ORDER_ID,
        TRIM(UPPER(CUSTOMER_ID))                        AS CUSTOMER_ID,

        -- ===== Descriptive Attributes (Satellite: Order Details) =====
        ORDER_DATE,
        TRIM(UPPER(ORDER_STATUS))                       AS ORDER_STATUS,
        TRIM(UPPER(SHIPPING_METHOD))                    AS SHIPPING_METHOD,
        TRIM(SHIPPING_ADDRESS)                          AS SHIPPING_ADDRESS,
        TRIM(BILLING_ADDRESS)                           AS BILLING_ADDRESS,
        FULFILLMENT_DATE,

        -- ===== Descriptive Attributes (Satellite: Order Financials) =====
        COALESCE(TOTAL_AMOUNT, 0)                       AS TOTAL_AMOUNT,
        TRIM(UPPER(CURRENCY_CODE))                      AS CURRENCY_CODE,
        TRIM(UPPER(PAYMENT_METHOD))                     AS PAYMENT_METHOD,
        COALESCE(DISCOUNT_AMOUNT, 0)                    AS DISCOUNT_AMOUNT,
        COALESCE(TAX_AMOUNT, 0)                         AS TAX_AMOUNT,

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
        {{ dbt_utils.generate_surrogate_key(['CUSTOMER_ID']) }}
                                                        AS HK_CUSTOMER,

        -- ===== Hash Key (Link: Customer ↔ Order) =====
        {{ dbt_utils.generate_surrogate_key(['CUSTOMER_ID', 'ORDER_ID']) }}
                                                        AS HK_LINK_CUSTOMER_ORDER,

        -- ===== Hash Diff (Satellite: Order Details) =====
        {{ dbt_utils.generate_surrogate_key([
            'ORDER_STATUS',
            'SHIPPING_METHOD',
            'SHIPPING_ADDRESS',
            'BILLING_ADDRESS',
            'FULFILLMENT_DATE'
        ]) }}                                           AS HD_ORDER_DETAILS,

        -- ===== Hash Diff (Satellite: Order Financials) =====
        {{ dbt_utils.generate_surrogate_key([
            'TOTAL_AMOUNT',
            'CURRENCY_CODE',
            'PAYMENT_METHOD',
            'DISCOUNT_AMOUNT',
            'TAX_AMOUNT'
        ]) }}                                           AS HD_ORDER_FINANCIALS,

        -- ===== Load Metadata =====
        _LOADED_AT                                      AS LOAD_DATETIME,
        'ECOMMERCE.RAW_ORDERS'                          AS RECORD_SOURCE,
        CURRENT_DATE()                                  AS APPLIED_DATE

    FROM renamed

)

SELECT * FROM hashed
