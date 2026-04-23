-- test_vault_referential_integrity.sql
-- Singular test: Verify every link FK exists in its parent hub.
-- This is a catch-all referential integrity test across the Raw Vault.

-- Check LINK_CUSTOMER_ORDER → HUB_CUSTOMER
SELECT 'LINK_CUSTOMER_ORDER.HK_CUSTOMER' AS CHECK_NAME, HK_CUSTOMER AS ORPHAN_KEY
FROM {{ ref('link_customer_order') }}
WHERE HK_CUSTOMER NOT IN (SELECT HK_CUSTOMER FROM {{ ref('hub_customer') }})

UNION ALL

-- Check LINK_CUSTOMER_ORDER → HUB_ORDER
SELECT 'LINK_CUSTOMER_ORDER.HK_ORDER', HK_ORDER
FROM {{ ref('link_customer_order') }}
WHERE HK_ORDER NOT IN (SELECT HK_ORDER FROM {{ ref('hub_order') }})

UNION ALL

-- Check LINK_ORDER_PRODUCT → HUB_ORDER
SELECT 'LINK_ORDER_PRODUCT.HK_ORDER', HK_ORDER
FROM {{ ref('link_order_product') }}
WHERE HK_ORDER NOT IN (SELECT HK_ORDER FROM {{ ref('hub_order') }})

UNION ALL

-- Check LINK_ORDER_PRODUCT → HUB_PRODUCT
SELECT 'LINK_ORDER_PRODUCT.HK_PRODUCT', HK_PRODUCT
FROM {{ ref('link_order_product') }}
WHERE HK_PRODUCT NOT IN (SELECT HK_PRODUCT FROM {{ ref('hub_product') }})
