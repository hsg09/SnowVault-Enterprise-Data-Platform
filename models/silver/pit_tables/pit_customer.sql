{{/*
  pit_customer.sql — Point-In-Time Table: Customer
  
  PURPOSE: Pre-computed lookup table that stores the correct satellite
           hash key + load datetime for each customer at each load point.
           Eliminates the expensive "latest record" window functions
           when joining multiple satellites.
  
  PATTERN: For each LOAD_DATETIME in the hub, find the most recent
           satellite record in each satellite that was loaded on or before.
*/}}

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['HK_CUSTOMER', 'PIT_LOAD_DATETIME'],
        on_schema_change='sync_all_columns'
    )
}}

WITH hub_dates AS (

    -- All distinct load datetimes from the hub
    SELECT DISTINCT
        HK_CUSTOMER,
        LOAD_DATETIME AS PIT_LOAD_DATETIME
    FROM {{ ref('hub_customer') }}

    UNION

    -- Also include satellite load datetimes for finer granularity
    SELECT DISTINCT
        HK_CUSTOMER,
        LOAD_DATETIME AS PIT_LOAD_DATETIME
    FROM {{ ref('sat_customer_details') }}

    UNION

    SELECT DISTINCT
        HK_CUSTOMER,
        LOAD_DATETIME AS PIT_LOAD_DATETIME
    FROM {{ ref('sat_customer_demographics') }}

),

pit AS (

    SELECT
        hd.HK_CUSTOMER,
        hd.PIT_LOAD_DATETIME,

        -- SAT_CUSTOMER_DETAILS: Find the most recent record at this point in time
        COALESCE(
            scd.LOAD_DATETIME,
            CAST('1900-01-01' AS TIMESTAMP_NTZ)
        )                                               AS SAT_CUSTOMER_DETAILS_LDTS,
        COALESCE(
            scd.HASH_DIFF,
            '{{ var("ghost_record_hash_key") }}'
        )                                               AS SAT_CUSTOMER_DETAILS_HD,

        -- SAT_CUSTOMER_DEMOGRAPHICS: Find the most recent record
        COALESCE(
            scdm.LOAD_DATETIME,
            CAST('1900-01-01' AS TIMESTAMP_NTZ)
        )                                               AS SAT_CUSTOMER_DEMOGRAPHICS_LDTS,
        COALESCE(
            scdm.HASH_DIFF,
            '{{ var("ghost_record_hash_key") }}'
        )                                               AS SAT_CUSTOMER_DEMOGRAPHICS_HD

    FROM hub_dates hd

    -- Latest SAT_CUSTOMER_DETAILS at or before PIT date
    LEFT JOIN LATERAL (
        SELECT HASH_DIFF, LOAD_DATETIME
        FROM {{ ref('sat_customer_details') }}
        WHERE HK_CUSTOMER = hd.HK_CUSTOMER
          AND LOAD_DATETIME <= hd.PIT_LOAD_DATETIME
        ORDER BY LOAD_DATETIME DESC
        LIMIT 1
    ) scd

    -- Latest SAT_CUSTOMER_DEMOGRAPHICS at or before PIT date
    LEFT JOIN LATERAL (
        SELECT HASH_DIFF, LOAD_DATETIME
        FROM {{ ref('sat_customer_demographics') }}
        WHERE HK_CUSTOMER = hd.HK_CUSTOMER
          AND LOAD_DATETIME <= hd.PIT_LOAD_DATETIME
        ORDER BY LOAD_DATETIME DESC
        LIMIT 1
    ) scdm

)

SELECT * FROM pit
