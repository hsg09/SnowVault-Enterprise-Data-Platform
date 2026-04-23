{{/*
  eff_sat_customer_order.sql — EFF_SAT_CUSTOMER_ORDER
  Data Vault 2.0 Effectivity Satellite on LINK_CUSTOMER_ORDER.
  
  PURPOSE: Track when a customer-order relationship is active or inactive.
           Enables querying "which orders were active for a customer at time X?"
  
  GRAIN: One row per (HK_LINK_CUSTOMER_ORDER, LOAD_DATETIME).
  PARENT: LINK_CUSTOMER_ORDER
  
  An effectivity satellite tracks the validity window of a link relationship.
  When an order is cancelled, a new row is inserted with IS_ACTIVE = FALSE.
*/}}

{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        unique_key=['HK_LINK_CUSTOMER_ORDER', 'LOAD_DATETIME'],
        on_schema_change='fail'
    )
}}

WITH staged_orders AS (

    SELECT
        HK_LINK_CUSTOMER_ORDER,
        HK_CUSTOMER,
        HK_ORDER,
        ORDER_STATUS,
        ORDER_DATE,
        LOAD_DATETIME,
        RECORD_SOURCE
    FROM {{ ref('stg_ecommerce__orders') }}

),

effectivity AS (

    SELECT
        HK_LINK_CUSTOMER_ORDER,
        HK_CUSTOMER,
        HK_ORDER,

        -- Determine relationship validity
        CASE
            WHEN ORDER_STATUS IN ('CANCELLED', 'RETURNED', 'REFUNDED')
                THEN FALSE
            ELSE TRUE
        END                                             AS IS_ACTIVE,

        -- Effective period
        ORDER_DATE                                      AS EFFECTIVE_FROM,
        CASE
            WHEN ORDER_STATUS IN ('CANCELLED', 'RETURNED', 'REFUNDED')
                THEN LOAD_DATETIME
            ELSE CAST('9999-12-31' AS TIMESTAMP_NTZ)    -- Open-ended (still active)
        END                                             AS EFFECTIVE_TO,

        ORDER_STATUS,
        LOAD_DATETIME,
        RECORD_SOURCE

    FROM staged_orders

),

{% if is_incremental() %}
latest_records AS (

    SELECT
        HK_LINK_CUSTOMER_ORDER,
        IS_ACTIVE
    FROM (
        SELECT
            HK_LINK_CUSTOMER_ORDER,
            IS_ACTIVE,
            ROW_NUMBER() OVER (
                PARTITION BY HK_LINK_CUSTOMER_ORDER
                ORDER BY LOAD_DATETIME DESC
            ) AS ROW_NUM
        FROM {{ this }}
    )
    WHERE ROW_NUM = 1

),
{% endif %}

new_records AS (

    SELECT
        eff.HK_LINK_CUSTOMER_ORDER,
        eff.HK_CUSTOMER,
        eff.HK_ORDER,
        eff.IS_ACTIVE,
        eff.EFFECTIVE_FROM,
        eff.EFFECTIVE_TO,
        eff.ORDER_STATUS,
        eff.LOAD_DATETIME,
        eff.RECORD_SOURCE

    FROM effectivity eff

    {% if is_incremental() %}
    LEFT JOIN latest_records lr
        ON eff.HK_LINK_CUSTOMER_ORDER = lr.HK_LINK_CUSTOMER_ORDER

    WHERE lr.HK_LINK_CUSTOMER_ORDER IS NULL     -- New relationship
       OR eff.IS_ACTIVE != lr.IS_ACTIVE          -- Status change (active ↔ inactive)
    {% endif %}

)

SELECT * FROM new_records
