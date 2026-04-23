-- =============================================================================
-- 18_cortex_ai_and_snowpark.sql — Snowflake Cortex AI + Snowpark Integration
-- Data Vault 2.0 Snowflake Data Platform
--
-- PURPOSE: Integrate Snowflake Cortex AI (LLM functions) and Snowpark
--          for advanced analytics, ML feature engineering, and AI-ready
--          semantic layer enhancement.
--
-- FEATURES:
--   - Cortex LLM functions (COMPLETE, CLASSIFY_TEXT, SENTIMENT, SUMMARIZE)
--   - Cortex Search for semantic search over enterprise data
--   - Snowpark UDFs/UDTFs for Python-based transformations
--   - ML feature store patterns
--
-- REQUIRES: ACCOUNTADMIN (for Cortex enablement)
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- 1. CORTEX AI — LLM Functions for Data Enrichment
-- =============================================================================

-- Sentiment analysis on customer feedback (Gold/Semantic layer)
CREATE OR REPLACE SECURE VIEW ANALYTICS.SEMANTIC_VIEWS.V_CUSTOMER_SENTIMENT AS
SELECT
    h.CUSTOMER_ID,
    sd.FIRST_NAME,
    sd.LAST_NAME,
    sd.EMAIL,
    -- Cortex AI: Classify customer segment using LLM
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        'Classify this customer profile into one of: HIGH_VALUE, GROWTH, AT_RISK, CHURNED. '
        || 'Segment: ' || COALESCE(sdm.CUSTOMER_SEGMENT, 'UNKNOWN')
        || ', Loyalty: ' || COALESCE(sdm.LOYALTY_TIER, 'NONE')
        || ', Orders: ' || COALESCE(order_stats.ORDER_COUNT::VARCHAR, '0'),
        ARRAY_CONSTRUCT('HIGH_VALUE', 'GROWTH', 'AT_RISK', 'CHURNED')
    ) AS AI_CUSTOMER_CLASS,
    -- Cortex AI: Generate customer summary
    SNOWFLAKE.CORTEX.SUMMARIZE(
        'Customer ' || sd.FIRST_NAME || ' ' || sd.LAST_NAME
        || ' is in segment ' || COALESCE(sdm.CUSTOMER_SEGMENT, 'UNKNOWN')
        || ' with loyalty tier ' || COALESCE(sdm.LOYALTY_TIER, 'NONE')
        || ' and has placed ' || COALESCE(order_stats.ORDER_COUNT::VARCHAR, '0') || ' orders.'
    ) AS AI_CUSTOMER_SUMMARY,
    order_stats.ORDER_COUNT,
    order_stats.TOTAL_REVENUE,
    sdm.CUSTOMER_SEGMENT,
    sdm.LOYALTY_TIER,
    CURRENT_TIMESTAMP() AS ENRICHED_AT
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
        SUM(sof.TOTAL_AMOUNT) AS TOTAL_REVENUE
    FROM RAW_VAULT.RAW_VAULT.LINK_CUSTOMER_ORDER l
    LEFT JOIN (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY HK_ORDER ORDER BY LOAD_DATETIME DESC) AS RN
        FROM RAW_VAULT.RAW_VAULT.SAT_ORDER_FINANCIALS
    ) sof ON l.HK_ORDER = sof.HK_ORDER AND sof.RN = 1
    GROUP BY l.HK_CUSTOMER
) order_stats ON h.HK_CUSTOMER = order_stats.HK_CUSTOMER;

GRANT SELECT ON VIEW ANALYTICS.SEMANTIC_VIEWS.V_CUSTOMER_SENTIMENT TO ROLE ANALYST;
GRANT SELECT ON VIEW ANALYTICS.SEMANTIC_VIEWS.V_CUSTOMER_SENTIMENT TO ROLE DATA_ENGINEER;

-- =============================================================================
-- 2. CORTEX SEARCH — Semantic Search Service
-- =============================================================================

-- Create Cortex Search service over product catalog
CREATE OR REPLACE CORTEX SEARCH SERVICE ANALYTICS.SEMANTIC_VIEWS.PRODUCT_SEARCH
    ON PRODUCT_DESCRIPTION
    WAREHOUSE = ANALYTICS_WH
    TARGET_LAG = '1 hour'
    COMMENT = 'Cortex Search: Semantic search over product catalog'
AS
SELECT
    h.PRODUCT_ID,
    sd.PRODUCT_NAME,
    sd.CATEGORY,
    sd.SUBCATEGORY,
    sd.BRAND,
    sd.PRODUCT_NAME || ' ' || COALESCE(sd.CATEGORY, '') || ' '
        || COALESCE(sd.SUBCATEGORY, '') || ' ' || COALESCE(sd.BRAND, '')
        AS PRODUCT_DESCRIPTION,
    sp.UNIT_PRICE,
    sp.COST_PRICE
FROM RAW_VAULT.RAW_VAULT.HUB_PRODUCT h
LEFT JOIN (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY HK_PRODUCT ORDER BY LOAD_DATETIME DESC) AS RN
    FROM RAW_VAULT.RAW_VAULT.SAT_PRODUCT_DETAILS
) sd ON h.HK_PRODUCT = sd.HK_PRODUCT AND sd.RN = 1
LEFT JOIN (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY HK_PRODUCT ORDER BY LOAD_DATETIME DESC) AS RN
    FROM RAW_VAULT.RAW_VAULT.SAT_PRODUCT_PRICING
) sp ON h.HK_PRODUCT = sp.HK_PRODUCT AND sp.RN = 1;

-- =============================================================================
-- 3. SNOWPARK UDF — Python-Based Feature Engineering
-- =============================================================================

-- Snowpark Python UDF: Calculate customer lifetime value (CLV) prediction
CREATE OR REPLACE FUNCTION ANALYTICS.SEMANTIC_VIEWS.PREDICT_CLV(
    order_count NUMBER,
    total_revenue FLOAT,
    avg_order_value FLOAT,
    days_since_first_order NUMBER,
    loyalty_tier VARCHAR
)
RETURNS FLOAT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('numpy')
HANDLER = 'predict_clv'
COMMENT = 'Snowpark UDF: Predict Customer Lifetime Value using probability model'
AS $$
import numpy as np

def predict_clv(order_count, total_revenue, avg_order_value, days_since_first_order, loyalty_tier):
    """
    Simple BG/NBD-inspired CLV prediction.
    In production, replace with a trained ML model via Snowpark ML.
    """
    if order_count is None or order_count == 0:
        return 0.0

    # Loyalty tier multiplier
    tier_multipliers = {
        'PLATINUM': 1.5, 'GOLD': 1.3, 'SILVER': 1.1, 'BRONZE': 1.0
    }
    multiplier = tier_multipliers.get(loyalty_tier, 1.0)

    # Recency factor (decay)
    recency = max(days_since_first_order or 1, 1)
    purchase_rate = order_count / (recency / 365.0)

    # Projected 12-month CLV
    projected_orders = purchase_rate * 1.0  # 1 year projection
    clv = projected_orders * (avg_order_value or 0) * multiplier

    return round(float(clv), 2)
$$;

GRANT USAGE ON FUNCTION ANALYTICS.SEMANTIC_VIEWS.PREDICT_CLV(
    NUMBER, FLOAT, FLOAT, NUMBER, VARCHAR
) TO ROLE ANALYST;

-- =============================================================================
-- 4. ML FEATURE STORE — Centralized Feature Table
-- =============================================================================

CREATE OR REPLACE DYNAMIC TABLE ANALYTICS.SEMANTIC_VIEWS.DYN_FEATURE_STORE_CUSTOMER
    TARGET_LAG = '1 hour'
    WAREHOUSE  = TRANSFORMER_WH
    REFRESH_MODE = AUTO
    INITIALIZE = ON_CREATE
    COMMENT = 'ML Feature Store: Customer features for model training and inference'
AS
SELECT
    h.CUSTOMER_ID,
    h.HK_CUSTOMER,
    -- Demographic features
    sdm.CUSTOMER_SEGMENT,
    sdm.LOYALTY_TIER,
    -- Order behavior features
    COALESCE(order_stats.ORDER_COUNT, 0) AS FEATURE_ORDER_COUNT,
    COALESCE(order_stats.TOTAL_REVENUE, 0) AS FEATURE_TOTAL_REVENUE,
    COALESCE(order_stats.AVG_ORDER_VALUE, 0) AS FEATURE_AVG_ORDER_VALUE,
    COALESCE(order_stats.TOTAL_DISCOUNT, 0) AS FEATURE_TOTAL_DISCOUNT,
    DATEDIFF('day', h.LOAD_DATETIME, CURRENT_DATE()) AS FEATURE_DAYS_SINCE_REGISTRATION,
    COALESCE(order_stats.DAYS_SINCE_LAST_ORDER, 999) AS FEATURE_DAYS_SINCE_LAST_ORDER,
    -- Predicted CLV
    ANALYTICS.SEMANTIC_VIEWS.PREDICT_CLV(
        COALESCE(order_stats.ORDER_COUNT, 0),
        COALESCE(order_stats.TOTAL_REVENUE, 0),
        COALESCE(order_stats.AVG_ORDER_VALUE, 0),
        DATEDIFF('day', h.LOAD_DATETIME, CURRENT_DATE()),
        sdm.LOYALTY_TIER
    ) AS FEATURE_PREDICTED_CLV,
    CURRENT_TIMESTAMP() AS FEATURE_COMPUTED_AT
FROM RAW_VAULT.RAW_VAULT.HUB_CUSTOMER h
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
        SUM(sof.DISCOUNT_AMOUNT) AS TOTAL_DISCOUNT,
        DATEDIFF('day', MAX(sof.LOAD_DATETIME), CURRENT_DATE()) AS DAYS_SINCE_LAST_ORDER
    FROM RAW_VAULT.RAW_VAULT.LINK_CUSTOMER_ORDER l
    LEFT JOIN (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY HK_ORDER ORDER BY LOAD_DATETIME DESC) AS RN
        FROM RAW_VAULT.RAW_VAULT.SAT_ORDER_FINANCIALS
    ) sof ON l.HK_ORDER = sof.HK_ORDER AND sof.RN = 1
    GROUP BY l.HK_CUSTOMER
) order_stats ON h.HK_CUSTOMER = order_stats.HK_CUSTOMER;

GRANT SELECT ON DYNAMIC TABLE ANALYTICS.SEMANTIC_VIEWS.DYN_FEATURE_STORE_CUSTOMER TO ROLE ANALYST;
GRANT SELECT ON DYNAMIC TABLE ANALYTICS.SEMANTIC_VIEWS.DYN_FEATURE_STORE_CUSTOMER TO ROLE DATA_ENGINEER;
