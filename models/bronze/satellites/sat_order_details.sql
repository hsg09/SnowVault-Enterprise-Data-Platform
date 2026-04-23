{{/*
  sat_order_details.sql — SAT_ORDER_DETAILS
  Data Vault 2.0 Satellite: Order status, shipping, and fulfilment details.
  
  GRAIN: One row per (HK_ORDER, LOAD_DATETIME) — full SCD Type 2 history.
  PARENT: HUB_ORDER
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
        HD_ORDER_DETAILS            AS HASH_DIFF,
        ORDER_DATE,
        ORDER_STATUS,
        SHIPPING_METHOD,
        SHIPPING_ADDRESS,
        BILLING_ADDRESS,
        FULFILLMENT_DATE,
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
        stg.ORDER_DATE,
        stg.ORDER_STATUS,
        stg.SHIPPING_METHOD,
        stg.SHIPPING_ADDRESS,
        stg.BILLING_ADDRESS,
        stg.FULFILLMENT_DATE,
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
