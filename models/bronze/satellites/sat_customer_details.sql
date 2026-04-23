{{/*
  sat_customer_details.sql — SAT_CUSTOMER_DETAILS
  Data Vault 2.0 Satellite: Customer contact and address details.
  
  GRAIN: One row per (HK_CUSTOMER, LOAD_DATETIME) — full SCD Type 2 history.
  CHANGE DETECTION: Hash diff (HD_CUSTOMER_DETAILS) comparison.
  PARENT: HUB_CUSTOMER
*/}}

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key=['HK_CUSTOMER', 'LOAD_DATETIME'],
        on_schema_change='fail'
    )
}}

WITH staging AS (

    SELECT
        HK_CUSTOMER,
        HD_CUSTOMER_DETAILS     AS HASH_DIFF,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        PHONE,
        COUNTRY_CODE,
        CITY,
        STATE,
        POSTAL_CODE,
        REGISTRATION_DATE,
        LOAD_DATETIME,
        RECORD_SOURCE
    FROM {{ ref('stg_ecommerce__customers') }}

),

{% if is_incremental() %}
-- Get the latest hash diff per business key from existing satellite
latest_records AS (

    SELECT
        HK_CUSTOMER,
        HASH_DIFF
    FROM (
        SELECT
            HK_CUSTOMER,
            HASH_DIFF,
            ROW_NUMBER() OVER (
                PARTITION BY HK_CUSTOMER
                ORDER BY LOAD_DATETIME DESC
            ) AS ROW_NUM
        FROM {{ this }}
    )
    WHERE ROW_NUM = 1

),
{% endif %}

-- Filter to only changed records (hash diff doesn't match latest)
new_records AS (

    SELECT
        stg.HK_CUSTOMER,
        stg.HASH_DIFF,
        stg.FIRST_NAME,
        stg.LAST_NAME,
        stg.EMAIL,
        stg.PHONE,
        stg.COUNTRY_CODE,
        stg.CITY,
        stg.STATE,
        stg.POSTAL_CODE,
        stg.REGISTRATION_DATE,
        stg.LOAD_DATETIME,
        stg.RECORD_SOURCE

    FROM staging stg

    {% if is_incremental() %}
    LEFT JOIN latest_records lr
        ON stg.HK_CUSTOMER = lr.HK_CUSTOMER

    WHERE lr.HK_CUSTOMER IS NULL            -- New business key (first load)
       OR stg.HASH_DIFF != lr.HASH_DIFF     -- Changed attributes
    {% endif %}

)

SELECT * FROM new_records
