{% snapshot snap_sat_customer_details %}
{#
    SCD Type 2 snapshot on SAT_CUSTOMER_DETAILS.
    Captures full history of customer detail changes over time.
    Uses timestamp-based strategy with LOAD_DATETIME.
#}

{{
    config(
        target_schema='snapshots',
        target_database=var('raw_vault_database'),
        unique_key='HK_CUSTOMER',
        strategy='timestamp',
        updated_at='LOAD_DATETIME',
        invalidate_hard_deletes=True
    )
}}

SELECT
    HK_CUSTOMER,
    HASH_DIFF,
    FIRST_NAME,
    LAST_NAME,
    EMAIL,
    PHONE,
    COUNTRY_CODE,
    CITY,
    STATE,
    POSTAL_CODE,
    REGISTRATION_DATE,
    LOAD_DATETIME,
    RECORD_SOURCE
FROM {{ ref('sat_customer_details') }}
-- Only latest record per customer (snapshot tracks changes over time)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY HK_CUSTOMER ORDER BY LOAD_DATETIME DESC
) = 1

{% endsnapshot %}
