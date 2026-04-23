{{/*
  conformed_customers.sql — Conformed Integration Layer: Customers
  
  PURPOSE: Flatten Data Vault structures into a single denormalized customer
           record for Gold layer consumption. Combines hub, latest satellites,
           and business vault classifications.
*/}}

{{
    config(
        materialized='table'
    )
}}

WITH hub AS (

    SELECT
        HK_CUSTOMER,
        CUSTOMER_ID,
        LOAD_DATETIME AS FIRST_LOADED_AT
    FROM {{ ref('hub_customer') }}

),

details AS (

    SELECT *
    FROM {{ ref('sat_customer_details') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY HK_CUSTOMER ORDER BY LOAD_DATETIME DESC
    ) = 1

),

demographics AS (

    SELECT *
    FROM {{ ref('sat_customer_demographics') }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY HK_CUSTOMER ORDER BY LOAD_DATETIME DESC
    ) = 1

),

classification AS (

    SELECT *
    FROM {{ ref('bv_customer_classification') }}

)

SELECT
    hub.HK_CUSTOMER,
    hub.CUSTOMER_ID,
    hub.FIRST_LOADED_AT,

    -- Contact details
    det.FIRST_NAME,
    det.LAST_NAME,
    det.EMAIL,
    det.PHONE,
    det.COUNTRY_CODE,
    det.CITY,
    det.STATE,
    det.POSTAL_CODE,
    det.REGISTRATION_DATE,

    -- Demographics
    dem.DATE_OF_BIRTH,
    dem.GENDER,
    dem.CUSTOMER_SEGMENT,
    dem.LOYALTY_TIER,

    -- Business vault classifications
    cls.TOTAL_ORDERS,
    cls.TOTAL_REVENUE,
    cls.AVG_ORDER_VALUE,
    cls.LAST_ORDER_DATE,
    cls.DAYS_SINCE_LAST_ORDER,
    cls.RECENCY_SCORE,
    cls.FREQUENCY_SCORE,
    cls.MONETARY_SCORE,
    cls.RFM_TOTAL_SCORE,
    cls.RFM_SEGMENT,
    cls.LTV_TIER,
    cls.CHURN_RISK,

    -- Audit
    det.LOAD_DATETIME   AS DETAILS_LAST_UPDATED,
    dem.LOAD_DATETIME   AS DEMOGRAPHICS_LAST_UPDATED,
    cls.CLASSIFIED_AT,
    CURRENT_TIMESTAMP() AS CONFORMED_AT

FROM hub
LEFT JOIN details det       ON hub.HK_CUSTOMER = det.HK_CUSTOMER
LEFT JOIN demographics dem  ON hub.HK_CUSTOMER = dem.HK_CUSTOMER
LEFT JOIN classification cls ON hub.HK_CUSTOMER = cls.HK_CUSTOMER
