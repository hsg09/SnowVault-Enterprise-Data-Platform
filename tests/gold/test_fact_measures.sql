-- test_fact_measures.sql
-- Verify fact table measures are within expected ranges.

SELECT *
FROM {{ ref('fct_orders') }}
WHERE TOTAL_AMOUNT < 0
   OR NET_AMOUNT < -1000    -- Allow small negative from adjustments
   OR DISCOUNT_AMOUNT < 0
   OR TAX_AMOUNT < 0
