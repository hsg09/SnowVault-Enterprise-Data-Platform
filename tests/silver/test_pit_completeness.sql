-- test_pit_completeness.sql
-- Verify PIT tables have an entry for every hub entity.

SELECT
    h.HK_CUSTOMER,
    h.CUSTOMER_ID
FROM {{ ref('hub_customer') }} h
LEFT JOIN {{ ref('pit_customer') }} p
    ON h.HK_CUSTOMER = p.HK_CUSTOMER
WHERE p.HK_CUSTOMER IS NULL
