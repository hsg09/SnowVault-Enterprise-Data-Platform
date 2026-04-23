{{/*
  sat_customer_demographics.sql — SAT_CUSTOMER_DEMOGRAPHICS
  Data Vault 2.0 Satellite: Customer demographic and segmentation data.
  
  GRAIN: One row per (HK_CUSTOMER, LOAD_DATETIME) — full SCD Type 2 history.
  CHANGE DETECTION: Hash diff (HD_CUSTOMER_DEMOGRAPHICS) comparison.
  PARENT: HUB_CUSTOMER
  
  RATIONALE: Separated from SAT_CUSTOMER_DETAILS because demographics and
             segmentation change at a different rate than contact details.
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
        HD_CUSTOMER_DEMOGRAPHICS    AS HASH_DIFF,
        DATE_OF_BIRTH,
        GENDER,
        CUSTOMER_SEGMENT,
        LOYALTY_TIER,
        LOAD_DATETIME,
        RECORD_SOURCE
    FROM {{ ref('stg_ecommerce__customers') }}

),

{% if is_incremental() %}
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

new_records AS (

    SELECT
        stg.HK_CUSTOMER,
        stg.HASH_DIFF,
        stg.DATE_OF_BIRTH,
        stg.GENDER,
        stg.CUSTOMER_SEGMENT,
        stg.LOYALTY_TIER,
        stg.LOAD_DATETIME,
        stg.RECORD_SOURCE

    FROM staging stg

    {% if is_incremental() %}
    LEFT JOIN latest_records lr
        ON stg.HK_CUSTOMER = lr.HK_CUSTOMER

    WHERE lr.HK_CUSTOMER IS NULL
       OR stg.HASH_DIFF != lr.HASH_DIFF
    {% endif %}

)

SELECT * FROM new_records
