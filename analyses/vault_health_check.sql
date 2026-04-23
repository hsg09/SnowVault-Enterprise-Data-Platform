-- vault_health_check.sql
-- Comprehensive health check across the Data Vault.
-- Run: dbt run-operation run_query --args "{'query': 'analyses/vault_health_check.sql'}"

-- ===== HUB COUNTS =====
SELECT 'HUB_CUSTOMER' AS ENTITY, COUNT(*) AS ROW_COUNT FROM {{ ref('hub_customer') }}
UNION ALL
SELECT 'HUB_ORDER', COUNT(*) FROM {{ ref('hub_order') }}
UNION ALL
SELECT 'HUB_PRODUCT', COUNT(*) FROM {{ ref('hub_product') }}
UNION ALL

-- ===== LINK COUNTS =====
SELECT 'LINK_CUSTOMER_ORDER', COUNT(*) FROM {{ ref('link_customer_order') }}
UNION ALL
SELECT 'LINK_ORDER_PRODUCT', COUNT(*) FROM {{ ref('link_order_product') }}
UNION ALL

-- ===== SATELLITE COUNTS =====
SELECT 'SAT_CUSTOMER_DETAILS', COUNT(*) FROM {{ ref('sat_customer_details') }}
UNION ALL
SELECT 'SAT_CUSTOMER_DEMOGRAPHICS', COUNT(*) FROM {{ ref('sat_customer_demographics') }}
UNION ALL
SELECT 'SAT_ORDER_DETAILS', COUNT(*) FROM {{ ref('sat_order_details') }}
UNION ALL
SELECT 'SAT_ORDER_FINANCIALS', COUNT(*) FROM {{ ref('sat_order_financials') }}
UNION ALL
SELECT 'SAT_PRODUCT_DETAILS', COUNT(*) FROM {{ ref('sat_product_details') }}
UNION ALL
SELECT 'SAT_PRODUCT_PRICING', COUNT(*) FROM {{ ref('sat_product_pricing') }}

ORDER BY ENTITY
