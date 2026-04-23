-- =============================================================================
-- 12_semantic_views.sql — Snowflake Semantic Views (AI-Ready Gold Layer)
-- Data Vault 2.0 Snowflake Data Platform
--
-- PURPOSE: Create Semantic Views that provide machine-readable context for
--          Snowflake Cortex AI and external BI tools. Defines centralized
--          KPI logic (revenue, active users, churn) to prevent conflicting
--          definitions across tools.
--
-- BLUEPRINT ALIGNMENT: "metrics are defined centrally within Semantic Views...
--          acts as a hard guardrail, ensuring the LLM understands context"
--
-- EXECUTION ORDER: Run AFTER Gold layer models are deployed
-- REQUIRES: DATA_ENGINEER or PLATFORM_ADMIN
-- =============================================================================

USE ROLE DATA_ENGINEER;
USE DATABASE ANALYTICS;

CREATE SCHEMA IF NOT EXISTS ANALYTICS.SEMANTIC_VIEWS;

-- =============================================================================
-- 1. SEMANTIC VIEW: Customer 360
-- Unified customer metric definitions for Cortex AI
-- =============================================================================

CREATE OR REPLACE SEMANTIC VIEW ANALYTICS.SEMANTIC_VIEWS.SV_CUSTOMER_360
    COMMENT = 'Semantic View: Unified customer metrics — drives Cortex AI context'
AS
SELECT
    CUSTOMER_SK,
    CUSTOMER_ID,
    FULL_NAME,
    EMAIL,
    COUNTRY_CODE,
    CUSTOMER_SEGMENT,
    LOYALTY_TIER,
    RFM_SEGMENT,
    LTV_TIER,
    CHURN_RISK,
    IS_ACTIVE,
    TOTAL_ORDERS,
    TOTAL_REVENUE,
    AVG_ORDER_VALUE,
    LAST_ORDER_DATE,
    REGISTRATION_DATE
FROM ANALYTICS.DIMENSIONS.DIM_CUSTOMER

-- Semantic annotations for Cortex AI
SEMANTIC METRICS (
    METRIC total_revenue
        DESCRIPTION 'Total lifetime revenue from non-cancelled orders, excluding tax'
        TYPE SUM
        EXPRESSION TOTAL_REVENUE,

    METRIC avg_order_value
        DESCRIPTION 'Average order value across all completed orders'
        TYPE AVERAGE
        EXPRESSION AVG_ORDER_VALUE,

    METRIC total_orders
        DESCRIPTION 'Count of distinct orders placed (excludes cancelled/refunded)'
        TYPE SUM
        EXPRESSION TOTAL_ORDERS,

    METRIC active_customers
        DESCRIPTION 'Customers with at least one order in the last 365 days'
        TYPE COUNT
        EXPRESSION CASE WHEN IS_ACTIVE = TRUE THEN 1 ELSE 0 END,

    METRIC churn_rate
        DESCRIPTION 'Percentage of customers classified as HIGH churn risk'
        TYPE RATIO
        EXPRESSION CASE WHEN CHURN_RISK = 'HIGH' THEN 1 ELSE 0 END
)

SEMANTIC DIMENSIONS (
    DIMENSION country
        DESCRIPTION 'ISO 3166-1 alpha-2 country code'
        EXPRESSION COUNTRY_CODE,

    DIMENSION segment
        DESCRIPTION 'Customer business segment: B2B, B2C, Enterprise'
        EXPRESSION CUSTOMER_SEGMENT,

    DIMENSION loyalty_tier
        DESCRIPTION 'Loyalty program tier: Bronze, Silver, Gold, Platinum'
        EXPRESSION LOYALTY_TIER,

    DIMENSION rfm_segment
        DESCRIPTION 'RFM-based segmentation: Champion, Loyal, At Risk, Lost, etc.'
        EXPRESSION RFM_SEGMENT,

    DIMENSION ltv_tier
        DESCRIPTION 'Customer lifetime value tier: New, Bronze, Silver, Gold, Platinum'
        EXPRESSION LTV_TIER
);

-- =============================================================================
-- 2. SEMANTIC VIEW: Revenue Analytics
-- Centralized revenue KPI definitions
-- =============================================================================

CREATE OR REPLACE SEMANTIC VIEW ANALYTICS.SEMANTIC_VIEWS.SV_REVENUE_ANALYTICS
    COMMENT = 'Semantic View: Revenue KPIs — single source of truth for revenue metrics'
AS
SELECT
    ORDER_DATE_KEY,
    ORDER_DATE,
    CURRENCY_CODE,
    ORDER_STATUS,
    LIFECYCLE_STAGE,
    SHIPPING_METHOD,
    PAYMENT_METHOD,
    TOTAL_AMOUNT,
    DISCOUNT_AMOUNT,
    TAX_AMOUNT,
    NET_AMOUNT,
    SLA_MET,
    HOURS_TO_FULFILLMENT
FROM ANALYTICS.FACTS.FCT_ORDERS

SEMANTIC METRICS (
    METRIC gross_revenue
        DESCRIPTION 'Total order amount before discounts, including tax'
        TYPE SUM
        EXPRESSION TOTAL_AMOUNT,

    METRIC net_revenue
        DESCRIPTION 'Revenue after discounts and tax: total - discount + tax'
        TYPE SUM
        EXPRESSION NET_AMOUNT,

    METRIC total_discounts
        DESCRIPTION 'Sum of all discount amounts applied'
        TYPE SUM
        EXPRESSION DISCOUNT_AMOUNT,

    METRIC avg_fulfillment_hours
        DESCRIPTION 'Average hours from order placement to fulfillment'
        TYPE AVERAGE
        EXPRESSION HOURS_TO_FULFILLMENT,

    METRIC sla_compliance_rate
        DESCRIPTION 'Percentage of orders meeting shipping SLA (72h standard, 24h express)'
        TYPE RATIO
        EXPRESSION CASE WHEN SLA_MET = TRUE THEN 1 ELSE 0 END,

    METRIC order_count
        DESCRIPTION 'Total number of orders'
        TYPE COUNT
        EXPRESSION 1
)

SEMANTIC DIMENSIONS (
    DIMENSION order_date
        DESCRIPTION 'Date the order was placed'
        EXPRESSION ORDER_DATE
        TIME_DIMENSION TRUE,

    DIMENSION currency
        DESCRIPTION 'ISO 4217 currency code'
        EXPRESSION CURRENCY_CODE,

    DIMENSION lifecycle_stage
        DESCRIPTION 'Order lifecycle: Awaiting, Processing, In Transit, Complete, Cancelled'
        EXPRESSION LIFECYCLE_STAGE,

    DIMENSION payment_method
        DESCRIPTION 'Payment method: Credit Card, PayPal, Bank Transfer, etc.'
        EXPRESSION PAYMENT_METHOD,

    DIMENSION shipping_method
        DESCRIPTION 'Shipping tier: Standard (72h SLA), Express (24h SLA), Overnight'
        EXPRESSION SHIPPING_METHOD
);

-- =============================================================================
-- 3. SEMANTIC VIEW: Product Performance
-- Product-level metric definitions
-- =============================================================================

CREATE OR REPLACE SEMANTIC VIEW ANALYTICS.SEMANTIC_VIEWS.SV_PRODUCT_PERFORMANCE
    COMMENT = 'Semantic View: Product performance metrics for Cortex AI'
AS
SELECT
    dp.PRODUCT_SK,
    dp.PRODUCT_ID,
    dp.PRODUCT_NAME,
    dp.CATEGORY,
    dp.SUBCATEGORY,
    dp.BRAND,
    dp.UNIT_PRICE,
    dp.COST_PRICE,
    dp.MARGIN_PERCENT,
    dp.IS_ACTIVE,
    fi.QUANTITY,
    fi.LINE_TOTAL,
    fi.GROSS_AMOUNT,
    fi.DISCOUNT_AMOUNT
FROM ANALYTICS.DIMENSIONS.DIM_PRODUCT dp
LEFT JOIN ANALYTICS.FACTS.FCT_ORDER_ITEMS fi
    ON dp.PRODUCT_SK = fi.PRODUCT_SK

SEMANTIC METRICS (
    METRIC units_sold
        DESCRIPTION 'Total units sold across all orders'
        TYPE SUM
        EXPRESSION QUANTITY,

    METRIC product_revenue
        DESCRIPTION 'Total revenue from product sales (line totals)'
        TYPE SUM
        EXPRESSION LINE_TOTAL,

    METRIC margin_percent
        DESCRIPTION 'Product profit margin: (unit_price - cost_price) / cost_price * 100'
        TYPE AVERAGE
        EXPRESSION MARGIN_PERCENT,

    METRIC avg_selling_price
        DESCRIPTION 'Average actual selling price after discounts'
        TYPE AVERAGE
        EXPRESSION LINE_TOTAL / NULLIF(QUANTITY, 0)
)

SEMANTIC DIMENSIONS (
    DIMENSION category
        DESCRIPTION 'Primary product category'
        EXPRESSION CATEGORY,

    DIMENSION subcategory
        DESCRIPTION 'Product subcategory'
        EXPRESSION SUBCATEGORY,

    DIMENSION brand
        DESCRIPTION 'Product brand name'
        EXPRESSION BRAND,

    DIMENSION is_active
        DESCRIPTION 'Whether product is currently active in catalog'
        EXPRESSION IS_ACTIVE
);

-- =============================================================================
-- GRANTS — Expose Semantic Views to Cortex AI and BI consumers
-- =============================================================================

GRANT USAGE ON SCHEMA ANALYTICS.SEMANTIC_VIEWS TO ROLE ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA ANALYTICS.SEMANTIC_VIEWS TO ROLE ANALYST;
GRANT SELECT ON ALL VIEWS IN SCHEMA ANALYTICS.SEMANTIC_VIEWS TO ROLE DATA_ENGINEER;
