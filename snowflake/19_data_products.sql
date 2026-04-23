-- =============================================================================
-- 19_data_products.sql — Data Products Catalog & Semantic Layer
-- Data Vault 2.0 Snowflake Data Platform
--
-- PURPOSE: Implement Data Products / Data Mesh patterns using Snowflake's
--          native Listings, Secure Views, and Semantic Model definitions.
--          Each data product is a self-contained, discoverable unit with
--          contracts (schema tests), SLAs (freshness), and access controls.
--
-- DATA MESH PRINCIPLES:
--   - Domain ownership (via DATA_DOMAIN tags)
--   - Data as a product (discoverable, documented, quality-assured)
--   - Self-serve platform (via RBAC + secure shares)
--   - Federated governance (via Horizon Catalog)
--
-- REQUIRES: ACCOUNTADMIN
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- 1. DATA PRODUCT REGISTRY — Catalog of published data products
-- =============================================================================

CREATE TABLE IF NOT EXISTS AUDIT.CONTROL.DATA_PRODUCT_REGISTRY (
    PRODUCT_ID              VARCHAR(64)     DEFAULT UUID_STRING() NOT NULL,
    PRODUCT_NAME            VARCHAR(500)    NOT NULL,
    PRODUCT_DOMAIN          VARCHAR(100)    NOT NULL,  -- CUSTOMER, ORDER, PRODUCT, FINANCIAL
    PRODUCT_OWNER           VARCHAR(250)    NOT NULL,
    PRODUCT_VERSION         VARCHAR(20)     DEFAULT '1.0.0',
    PRODUCT_TIER            VARCHAR(20)     DEFAULT 'STANDARD',  -- BRONZE, SILVER, GOLD, PLATINUM
    PRODUCT_TYPE            VARCHAR(50)     NOT NULL,  -- SECURE_VIEW | SHARE | API | DYNAMIC_TABLE
    DATABASE_NAME           VARCHAR(250)    NOT NULL,
    SCHEMA_NAME             VARCHAR(250)    NOT NULL,
    OBJECT_NAME             VARCHAR(500)    NOT NULL,
    DESCRIPTION             VARCHAR(4000),
    SLA_FRESHNESS_HOURS     NUMBER(6,0)     DEFAULT 24,
    SLA_UPTIME_PERCENT      NUMBER(5,2)     DEFAULT 99.5,
    QUALITY_SCORE           NUMBER(5,2),     -- 0-100, computed from dbt test results
    CONSUMERS               VARIANT,         -- Array of roles/users consuming this product
    CONTRACT_SCHEMA         VARIANT,         -- Expected schema definition (columns, types)
    IS_ACTIVE               BOOLEAN         DEFAULT TRUE,
    PUBLISHED_AT            TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    LAST_REFRESHED_AT       TIMESTAMP_NTZ,
    CREATED_AT              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_DATA_PRODUCT_REGISTRY PRIMARY KEY (PRODUCT_ID)
)
COMMENT = 'Data Product Registry — catalog of published, discoverable data products';

-- =============================================================================
-- 2. DATA PRODUCTS — Gold Layer Secure Views
-- =============================================================================

-- Data Product: Customer 360 (Gold)
CREATE OR REPLACE SECURE VIEW ANALYTICS.SECURE_VIEWS.DP_CUSTOMER_360
    COMMENT = 'Data Product: Customer 360 — unified customer profile with demographics, orders, and CLV'
AS
SELECT
    h.CUSTOMER_ID,
    sd.FIRST_NAME,
    sd.LAST_NAME,
    sd.EMAIL,
    sd.PHONE,
    sd.COUNTRY_CODE,
    sd.CITY,
    sd.STATE,
    sdm.CUSTOMER_SEGMENT,
    sdm.LOYALTY_TIER,
    COALESCE(os.ORDER_COUNT, 0) AS LIFETIME_ORDERS,
    COALESCE(os.TOTAL_REVENUE, 0) AS LIFETIME_REVENUE,
    COALESCE(os.AVG_ORDER_VALUE, 0) AS AVG_ORDER_VALUE,
    os.FIRST_ORDER_DATE,
    os.LAST_ORDER_DATE,
    h.LOAD_DATETIME AS FIRST_SEEN_DATE,
    CURRENT_TIMESTAMP() AS SNAPSHOT_AT
FROM RAW_VAULT.RAW_VAULT.HUB_CUSTOMER h
LEFT JOIN (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY HK_CUSTOMER ORDER BY LOAD_DATETIME DESC) AS RN
    FROM RAW_VAULT.RAW_VAULT.SAT_CUSTOMER_DETAILS
) sd ON h.HK_CUSTOMER = sd.HK_CUSTOMER AND sd.RN = 1
LEFT JOIN (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY HK_CUSTOMER ORDER BY LOAD_DATETIME DESC) AS RN
    FROM RAW_VAULT.RAW_VAULT.SAT_CUSTOMER_DEMOGRAPHICS
) sdm ON h.HK_CUSTOMER = sdm.HK_CUSTOMER AND sdm.RN = 1
LEFT JOIN (
    SELECT
        l.HK_CUSTOMER,
        COUNT(DISTINCT l.HK_ORDER) AS ORDER_COUNT,
        SUM(sof.TOTAL_AMOUNT) AS TOTAL_REVENUE,
        AVG(sof.TOTAL_AMOUNT) AS AVG_ORDER_VALUE,
        MIN(sof.LOAD_DATETIME) AS FIRST_ORDER_DATE,
        MAX(sof.LOAD_DATETIME) AS LAST_ORDER_DATE
    FROM RAW_VAULT.RAW_VAULT.LINK_CUSTOMER_ORDER l
    LEFT JOIN (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY HK_ORDER ORDER BY LOAD_DATETIME DESC) AS RN
        FROM RAW_VAULT.RAW_VAULT.SAT_ORDER_FINANCIALS
    ) sof ON l.HK_ORDER = sof.HK_ORDER AND sof.RN = 1
    GROUP BY l.HK_CUSTOMER
) os ON h.HK_CUSTOMER = os.HK_CUSTOMER;

-- Data Product: Product Performance (Gold)
CREATE OR REPLACE SECURE VIEW ANALYTICS.SECURE_VIEWS.DP_PRODUCT_PERFORMANCE
    COMMENT = 'Data Product: Product Performance — sales metrics, margin analysis, and pricing trends'
AS
SELECT
    h.PRODUCT_ID,
    sd.PRODUCT_NAME,
    sd.CATEGORY,
    sd.SUBCATEGORY,
    sd.BRAND,
    sp.UNIT_PRICE AS CURRENT_PRICE,
    sp.COST_PRICE,
    (sp.UNIT_PRICE - sp.COST_PRICE) / NULLIF(sp.UNIT_PRICE, 0) * 100 AS MARGIN_PERCENT,
    COALESCE(sales.UNITS_SOLD, 0) AS LIFETIME_UNITS_SOLD,
    COALESCE(sales.REVENUE, 0) AS LIFETIME_REVENUE,
    sd.IS_ACTIVE,
    CURRENT_TIMESTAMP() AS SNAPSHOT_AT
FROM RAW_VAULT.RAW_VAULT.HUB_PRODUCT h
LEFT JOIN (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY HK_PRODUCT ORDER BY LOAD_DATETIME DESC) AS RN
    FROM RAW_VAULT.RAW_VAULT.SAT_PRODUCT_DETAILS
) sd ON h.HK_PRODUCT = sd.HK_PRODUCT AND sd.RN = 1
LEFT JOIN (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY HK_PRODUCT ORDER BY LOAD_DATETIME DESC) AS RN
    FROM RAW_VAULT.RAW_VAULT.SAT_PRODUCT_PRICING
) sp ON h.HK_PRODUCT = sp.HK_PRODUCT AND sp.RN = 1
LEFT JOIN (
    SELECT
        l.HK_PRODUCT,
        SUM(oi.QUANTITY) AS UNITS_SOLD,
        SUM(oi.LINE_TOTAL) AS REVENUE
    FROM RAW_VAULT.RAW_VAULT.LINK_ORDER_PRODUCT l
    LEFT JOIN RAW_VAULT.ECOMMERCE.RAW_ORDER_ITEMS oi ON l.ORDER_ITEM_ID = oi.ORDER_ITEM_ID
    GROUP BY l.HK_PRODUCT
) sales ON h.HK_PRODUCT = sales.HK_PRODUCT;

-- =============================================================================
-- 3. REGISTER DATA PRODUCTS
-- =============================================================================

INSERT INTO AUDIT.CONTROL.DATA_PRODUCT_REGISTRY
    (PRODUCT_NAME, PRODUCT_DOMAIN, PRODUCT_OWNER, PRODUCT_TIER, PRODUCT_TYPE,
     DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, DESCRIPTION, SLA_FRESHNESS_HOURS)
SELECT * FROM VALUES
    ('Customer 360', 'CUSTOMER', 'data-platform-team', 'GOLD', 'SECURE_VIEW',
     'ANALYTICS', 'SECURE_VIEWS', 'DP_CUSTOMER_360',
     'Unified customer profile with demographics, order history, and lifetime value', 12),
    ('Product Performance', 'PRODUCT', 'data-platform-team', 'GOLD', 'SECURE_VIEW',
     'ANALYTICS', 'SECURE_VIEWS', 'DP_PRODUCT_PERFORMANCE',
     'Product sales metrics, margin analysis, and pricing trends', 24),
    ('Customer Feature Store', 'CUSTOMER', 'ml-engineering-team', 'PLATINUM', 'DYNAMIC_TABLE',
     'ANALYTICS', 'SEMANTIC_VIEWS', 'DYN_FEATURE_STORE_CUSTOMER',
     'ML feature store with predicted CLV and behavioral features', 1),
    ('Revenue by Segment', 'FINANCIAL', 'data-analytics-team', 'GOLD', 'DYNAMIC_TABLE',
     'ANALYTICS', 'AGGREGATES', 'DYN_AGG_REVENUE_BY_SEGMENT',
     'Real-time revenue aggregation by customer segment and loyalty tier', 1)
WHERE NOT EXISTS (
    SELECT 1 FROM AUDIT.CONTROL.DATA_PRODUCT_REGISTRY
    WHERE PRODUCT_NAME = 'Customer 360'
);

-- =============================================================================
-- 4. DATA PRODUCT DISCOVERY VIEW
-- =============================================================================

CREATE OR REPLACE VIEW AUDIT.CONTROL.DATA_PRODUCT_CATALOG AS
SELECT
    PRODUCT_NAME,
    PRODUCT_DOMAIN,
    PRODUCT_TIER,
    PRODUCT_TYPE,
    DATABASE_NAME || '.' || SCHEMA_NAME || '.' || OBJECT_NAME AS FULLY_QUALIFIED_NAME,
    DESCRIPTION,
    PRODUCT_OWNER,
    'SLA: ' || SLA_FRESHNESS_HOURS || 'h freshness, '
        || SLA_UPTIME_PERCENT || '% uptime' AS SLA_SUMMARY,
    QUALITY_SCORE,
    PRODUCT_VERSION,
    IS_ACTIVE,
    PUBLISHED_AT
FROM AUDIT.CONTROL.DATA_PRODUCT_REGISTRY
WHERE IS_ACTIVE = TRUE
ORDER BY PRODUCT_DOMAIN, PRODUCT_TIER DESC, PRODUCT_NAME;

GRANT SELECT ON VIEW AUDIT.CONTROL.DATA_PRODUCT_CATALOG TO ROLE ANALYST;
GRANT SELECT ON VIEW AUDIT.CONTROL.DATA_PRODUCT_CATALOG TO ROLE DATA_ENGINEER;
GRANT SELECT ON VIEW AUDIT.CONTROL.DATA_PRODUCT_CATALOG TO ROLE PLATFORM_ADMIN;

-- =============================================================================
-- 5. GRANTS — Data Product Access
-- =============================================================================

GRANT SELECT ON VIEW ANALYTICS.SECURE_VIEWS.DP_CUSTOMER_360 TO ROLE ANALYST;
GRANT SELECT ON VIEW ANALYTICS.SECURE_VIEWS.DP_CUSTOMER_360 TO ROLE DATA_ENGINEER;

GRANT SELECT ON VIEW ANALYTICS.SECURE_VIEWS.DP_PRODUCT_PERFORMANCE TO ROLE ANALYST;
GRANT SELECT ON VIEW ANALYTICS.SECURE_VIEWS.DP_PRODUCT_PERFORMANCE TO ROLE DATA_ENGINEER;
