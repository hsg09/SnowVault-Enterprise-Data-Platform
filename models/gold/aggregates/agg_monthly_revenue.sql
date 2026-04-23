{{/*
  agg_monthly_revenue.sql — Aggregate: Monthly Revenue
  
  GRAIN: One row per (year, month, currency).
  MEASURES: Revenue, order count, avg order value, new/returning customers.
*/}}

{{
    config(
        materialized='table'
    )
}}

WITH orders AS (

    SELECT
        fct.ORDER_DATE,
        fct.TOTAL_AMOUNT,
        fct.NET_AMOUNT,
        fct.DISCOUNT_AMOUNT,
        fct.TAX_AMOUNT,
        fct.CURRENCY_CODE,
        fct.ORDER_STATUS,
        fct.LIFECYCLE_STAGE,
        fct.HK_CUSTOMER,
        dim_d.CALENDAR_YEAR,
        dim_d.CALENDAR_MONTH,
        dim_d.YEAR_MONTH,
        dim_d.CALENDAR_QUARTER,
        dim_d.YEAR_QUARTER
    FROM {{ ref('fct_orders') }} fct
    INNER JOIN {{ ref('dim_date') }} dim_d
        ON fct.ORDER_DATE_KEY = dim_d.DATE_KEY
    WHERE fct.ORDER_STATUS NOT IN ('CANCELLED', 'REFUNDED')

),

-- Identify first-order customers per month
customer_first_order AS (

    SELECT
        HK_CUSTOMER,
        MIN(ORDER_DATE) AS FIRST_ORDER_DATE
    FROM orders
    GROUP BY HK_CUSTOMER

)

SELECT
    o.CALENDAR_YEAR,
    o.CALENDAR_MONTH,
    o.YEAR_MONTH,
    o.CALENDAR_QUARTER,
    o.YEAR_QUARTER,
    o.CURRENCY_CODE,

    -- Revenue measures
    COUNT(*)                                        AS TOTAL_ORDERS,
    SUM(o.TOTAL_AMOUNT)                             AS GROSS_REVENUE,
    SUM(o.NET_AMOUNT)                               AS NET_REVENUE,
    SUM(o.DISCOUNT_AMOUNT)                          AS TOTAL_DISCOUNTS,
    SUM(o.TAX_AMOUNT)                               AS TOTAL_TAX,
    AVG(o.TOTAL_AMOUNT)                             AS AVG_ORDER_VALUE,

    -- Customer measures
    COUNT(DISTINCT o.HK_CUSTOMER)                   AS UNIQUE_CUSTOMERS,
    COUNT(DISTINCT CASE
        WHEN DATE_TRUNC('month', cfo.FIRST_ORDER_DATE) = DATE_TRUNC('month', o.ORDER_DATE)
        THEN o.HK_CUSTOMER
    END)                                            AS NEW_CUSTOMERS,
    COUNT(DISTINCT CASE
        WHEN DATE_TRUNC('month', cfo.FIRST_ORDER_DATE) < DATE_TRUNC('month', o.ORDER_DATE)
        THEN o.HK_CUSTOMER
    END)                                            AS RETURNING_CUSTOMERS,

    -- Computed
    CURRENT_TIMESTAMP()                             AS AGGREGATED_AT

FROM orders o
LEFT JOIN customer_first_order cfo ON o.HK_CUSTOMER = cfo.HK_CUSTOMER
GROUP BY
    o.CALENDAR_YEAR,
    o.CALENDAR_MONTH,
    o.YEAR_MONTH,
    o.CALENDAR_QUARTER,
    o.YEAR_QUARTER,
    o.CURRENCY_CODE
