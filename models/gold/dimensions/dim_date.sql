{{/*
  dim_date.sql — Dimension: Date (Calendar)
  
  GRAIN: One row per calendar date.
  RANGE: 2020-01-01 to 2030-12-31 (generated via dbt_utils.date_spine).
  
  NOTE: This is a generated dimension, not sourced from raw data.
*/}}

{{
    config(
        materialized='table'
    )
}}

WITH date_spine AS (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}

),

dates AS (

    SELECT
        CAST(date_day AS DATE) AS DATE_DAY
    FROM date_spine

)

SELECT
    -- Primary key
    TO_VARCHAR(DATE_DAY, 'YYYYMMDD')                AS DATE_KEY,
    DATE_DAY,

    -- Calendar attributes
    YEAR(DATE_DAY)                                  AS CALENDAR_YEAR,
    QUARTER(DATE_DAY)                               AS CALENDAR_QUARTER,
    MONTH(DATE_DAY)                                 AS CALENDAR_MONTH,
    WEEKOFYEAR(DATE_DAY)                            AS CALENDAR_WEEK,
    DAYOFYEAR(DATE_DAY)                             AS DAY_OF_YEAR,
    DAYOFMONTH(DATE_DAY)                            AS DAY_OF_MONTH,
    DAYOFWEEK(DATE_DAY)                             AS DAY_OF_WEEK,
    DAYNAME(DATE_DAY)                               AS DAY_NAME,
    MONTHNAME(DATE_DAY)                             AS MONTH_NAME,

    -- Derived attributes
    CONCAT('Q', QUARTER(DATE_DAY))                  AS QUARTER_NAME,
    CONCAT(YEAR(DATE_DAY), '-Q', QUARTER(DATE_DAY)) AS YEAR_QUARTER,
    CONCAT(YEAR(DATE_DAY), '-', LPAD(MONTH(DATE_DAY), 2, '0'))
                                                    AS YEAR_MONTH,

    -- Boolean flags
    CASE WHEN DAYOFWEEK(DATE_DAY) IN (0, 6) THEN TRUE ELSE FALSE END
                                                    AS IS_WEEKEND,
    CASE WHEN DAYOFWEEK(DATE_DAY) IN (0, 6) THEN FALSE ELSE TRUE END
                                                    AS IS_WEEKDAY,
    CASE WHEN DAYOFMONTH(DATE_DAY) = 1 THEN TRUE ELSE FALSE END
                                                    AS IS_FIRST_OF_MONTH,
    CASE WHEN DATE_DAY = LAST_DAY(DATE_DAY) THEN TRUE ELSE FALSE END
                                                    AS IS_LAST_OF_MONTH,

    -- Relative dates
    CASE WHEN DATE_DAY = CURRENT_DATE() THEN TRUE ELSE FALSE END
                                                    AS IS_TODAY,
    DATEDIFF('day', DATE_DAY, CURRENT_DATE())       AS DAYS_AGO,

    -- Fiscal year (assuming April 1 fiscal year start)
    CASE
        WHEN MONTH(DATE_DAY) >= 4 THEN YEAR(DATE_DAY)
        ELSE YEAR(DATE_DAY) - 1
    END                                             AS FISCAL_YEAR,
    CASE
        WHEN MONTH(DATE_DAY) >= 4
            THEN CEIL((MONTH(DATE_DAY) - 3) / 3.0)
        ELSE CEIL((MONTH(DATE_DAY) + 9) / 3.0)
    END                                             AS FISCAL_QUARTER

FROM dates
