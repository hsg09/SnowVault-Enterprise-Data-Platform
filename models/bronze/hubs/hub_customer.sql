{{/*
  hub_customer.sql — HUB_CUSTOMER
  Data Vault 2.0 Hub: Unique business keys for customers.
  
  GRAIN: One row per unique CUSTOMER_ID (ever observed).
  PATTERN: Insert-only, no updates. First-seen record source and load datetime.
*/}}

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key='HK_CUSTOMER',
        on_schema_change='fail'
    )
}}

WITH staging AS (

    SELECT
        HK_CUSTOMER,
        CUSTOMER_ID,
        LOAD_DATETIME,
        RECORD_SOURCE
    FROM {{ ref('stg_ecommerce__customers') }}

    {% if is_incremental() %}
    WHERE LOAD_DATETIME > (SELECT COALESCE(MAX(LOAD_DATETIME), '1900-01-01') FROM {{ this }})
    {% endif %}

),

-- Deduplicate: keep the earliest record per business key
deduplicated AS (

    SELECT
        HK_CUSTOMER,
        CUSTOMER_ID,
        LOAD_DATETIME,
        RECORD_SOURCE,
        ROW_NUMBER() OVER (
            PARTITION BY HK_CUSTOMER
            ORDER BY LOAD_DATETIME ASC
        ) AS ROW_NUM

    FROM staging

)

SELECT
    HK_CUSTOMER,
    CUSTOMER_ID,
    LOAD_DATETIME,
    RECORD_SOURCE

FROM deduplicated
WHERE ROW_NUM = 1
{% if is_incremental() %}
  AND HK_CUSTOMER NOT IN (SELECT HK_CUSTOMER FROM {{ this }})
{% endif %}
