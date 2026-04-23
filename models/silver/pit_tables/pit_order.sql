{{/*
  pit_order.sql — Point-In-Time Table: Order
  
  PURPOSE: Pre-computed lookup for order satellites at any point in time.
           Covers SAT_ORDER_DETAILS and SAT_ORDER_FINANCIALS.
*/}}

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key=['HK_ORDER', 'PIT_LOAD_DATETIME'],
        on_schema_change='sync_all_columns'
    )
}}

WITH hub_dates AS (

    SELECT DISTINCT
        HK_ORDER,
        LOAD_DATETIME AS PIT_LOAD_DATETIME
    FROM {{ ref('hub_order') }}

    UNION

    SELECT DISTINCT
        HK_ORDER,
        LOAD_DATETIME AS PIT_LOAD_DATETIME
    FROM {{ ref('sat_order_details') }}

    UNION

    SELECT DISTINCT
        HK_ORDER,
        LOAD_DATETIME AS PIT_LOAD_DATETIME
    FROM {{ ref('sat_order_financials') }}

),

pit AS (

    SELECT
        hd.HK_ORDER,
        hd.PIT_LOAD_DATETIME,

        -- SAT_ORDER_DETAILS
        COALESCE(
            sod.LOAD_DATETIME,
            CAST('1900-01-01' AS TIMESTAMP_NTZ)
        )                                               AS SAT_ORDER_DETAILS_LDTS,

        -- SAT_ORDER_FINANCIALS
        COALESCE(
            sof.LOAD_DATETIME,
            CAST('1900-01-01' AS TIMESTAMP_NTZ)
        )                                               AS SAT_ORDER_FINANCIALS_LDTS

    FROM hub_dates hd

    LEFT JOIN LATERAL (
        SELECT LOAD_DATETIME
        FROM {{ ref('sat_order_details') }}
        WHERE HK_ORDER = hd.HK_ORDER
          AND LOAD_DATETIME <= hd.PIT_LOAD_DATETIME
        ORDER BY LOAD_DATETIME DESC
        LIMIT 1
    ) sod

    LEFT JOIN LATERAL (
        SELECT LOAD_DATETIME
        FROM {{ ref('sat_order_financials') }}
        WHERE HK_ORDER = hd.HK_ORDER
          AND LOAD_DATETIME <= hd.PIT_LOAD_DATETIME
        ORDER BY LOAD_DATETIME DESC
        LIMIT 1
    ) sof

)

SELECT * FROM pit
