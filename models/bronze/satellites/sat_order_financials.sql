{{/*
  sat_order_financials.sql — SAT_ORDER_FINANCIALS
  Data Vault 2.0 Satellite: Order financial attributes.
  
  GRAIN: One row per (HK_ORDER, LOAD_DATETIME) — full SCD Type 2 history.
  PARENT: HUB_ORDER
  
  RATIONALE: Separated from order details because financial attributes
             (amounts, currency, payment) rarely change after order placement.
*/}}

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key=['HK_ORDER', 'LOAD_DATETIME'],
        on_schema_change='fail'
    )
}}

WITH staging AS (

    SELECT
        HK_ORDER,
        HD_ORDER_FINANCIALS         AS HASH_DIFF,
        TOTAL_AMOUNT,
        CURRENCY_CODE,
        PAYMENT_METHOD,
        DISCOUNT_AMOUNT,
        TAX_AMOUNT,
        LOAD_DATETIME,
        RECORD_SOURCE
    FROM {{ ref('stg_ecommerce__orders') }}

),

{% if is_incremental() %}
latest_records AS (

    SELECT
        HK_ORDER,
        HASH_DIFF
    FROM (
        SELECT
            HK_ORDER,
            HASH_DIFF,
            ROW_NUMBER() OVER (
                PARTITION BY HK_ORDER
                ORDER BY LOAD_DATETIME DESC
            ) AS ROW_NUM
        FROM {{ this }}
    )
    WHERE ROW_NUM = 1

),
{% endif %}

new_records AS (

    SELECT
        stg.HK_ORDER,
        stg.HASH_DIFF,
        stg.TOTAL_AMOUNT,
        stg.CURRENCY_CODE,
        stg.PAYMENT_METHOD,
        stg.DISCOUNT_AMOUNT,
        stg.TAX_AMOUNT,
        stg.LOAD_DATETIME,
        stg.RECORD_SOURCE

    FROM staging stg

    {% if is_incremental() %}
    LEFT JOIN latest_records lr
        ON stg.HK_ORDER = lr.HK_ORDER

    WHERE lr.HK_ORDER IS NULL
       OR stg.HASH_DIFF != lr.HASH_DIFF
    {% endif %}

)

SELECT * FROM new_records
