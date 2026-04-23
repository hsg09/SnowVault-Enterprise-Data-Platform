-- pipeline_sla_monitor.sql
-- Monitor pipeline freshness and SLA compliance across all layers.

WITH layer_freshness AS (

    -- Bronze latest load
    SELECT 'BRONZE' AS LAYER, 'SAT_CUSTOMER_DETAILS' AS MODEL,
           MAX(LOAD_DATETIME) AS LATEST_RECORD,
           DATEDIFF('hour', MAX(LOAD_DATETIME), CURRENT_TIMESTAMP()) AS HOURS_AGO
    FROM {{ ref('sat_customer_details') }}

    UNION ALL

    SELECT 'BRONZE', 'SAT_ORDER_DETAILS',
           MAX(LOAD_DATETIME),
           DATEDIFF('hour', MAX(LOAD_DATETIME), CURRENT_TIMESTAMP())
    FROM {{ ref('sat_order_details') }}

    UNION ALL

    -- Silver latest compute
    SELECT 'SILVER', 'BV_CUSTOMER_CLASSIFICATION',
           MAX(CLASSIFIED_AT),
           DATEDIFF('hour', MAX(CLASSIFIED_AT), CURRENT_TIMESTAMP())
    FROM {{ ref('bv_customer_classification') }}

    UNION ALL

    -- Gold latest update
    SELECT 'GOLD', 'FCT_ORDERS',
           MAX(UPDATED_AT),
           DATEDIFF('hour', MAX(UPDATED_AT), CURRENT_TIMESTAMP())
    FROM {{ ref('fct_orders') }}

)

SELECT
    LAYER,
    MODEL,
    LATEST_RECORD,
    HOURS_AGO,
    CASE
        WHEN HOURS_AGO <= 6  THEN 'GREEN'
        WHEN HOURS_AGO <= 12 THEN 'YELLOW'
        WHEN HOURS_AGO <= 24 THEN 'ORANGE'
        ELSE 'RED'
    END AS SLA_STATUS
FROM layer_freshness
ORDER BY LAYER, MODEL
