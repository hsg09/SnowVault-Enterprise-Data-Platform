-- test_dim_uniqueness.sql
-- Verify all dimension surrogate keys are unique.

SELECT CUSTOMER_SK, COUNT(*) AS CNT
FROM {{ ref('dim_customer') }}
GROUP BY CUSTOMER_SK
HAVING COUNT(*) > 1

UNION ALL

SELECT PRODUCT_SK, COUNT(*)
FROM {{ ref('dim_product') }}
GROUP BY PRODUCT_SK
HAVING COUNT(*) > 1
