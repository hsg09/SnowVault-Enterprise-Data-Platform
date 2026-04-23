{{/*
  bridge_customer_order.sql — Bridge Table: Customer ↔ Order
  
  PURPOSE: Pre-computed link-walking table for the Customer → Order path.
           Joins hub, link, and effectivity satellite into a single flat
           structure for efficient downstream querying.
  
  INCLUDES: Only active (non-cancelled) relationships via effectivity satellite.
*/}}

{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='HK_LINK_CUSTOMER_ORDER',
        on_schema_change='sync_all_columns'
    )
}}

WITH bridge AS (

    SELECT
        lco.HK_LINK_CUSTOMER_ORDER,
        lco.HK_CUSTOMER,
        lco.HK_ORDER,
        lco.LOAD_DATETIME           AS LINK_LOAD_DATETIME,
        lco.RECORD_SOURCE           AS LINK_RECORD_SOURCE,

        -- Effectivity
        eff.IS_ACTIVE,
        eff.EFFECTIVE_FROM,
        eff.EFFECTIVE_TO,
        eff.ORDER_STATUS             AS EFF_ORDER_STATUS

    FROM {{ ref('link_customer_order') }} lco

    -- Join latest effectivity satellite record
    LEFT JOIN (
        SELECT *
        FROM {{ ref('eff_sat_customer_order') }}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY HK_LINK_CUSTOMER_ORDER
            ORDER BY LOAD_DATETIME DESC
        ) = 1
    ) eff
        ON lco.HK_LINK_CUSTOMER_ORDER = eff.HK_LINK_CUSTOMER_ORDER

)

SELECT * FROM bridge
