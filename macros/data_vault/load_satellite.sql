{% macro load_satellite(src_model, src_hash_key, src_hash_diff, src_payload_columns, src_load_datetime, src_record_source) %}
{#
    load_satellite — Generic satellite loading pattern with change detection.
    
    Usage:
        {{ load_satellite(
            src_model=ref('stg_ecommerce__customers'),
            src_hash_key='HK_CUSTOMER',
            src_hash_diff='HD_CUSTOMER_DETAILS',
            src_payload_columns=['FIRST_NAME', 'LAST_NAME', 'EMAIL', 'PHONE'],
            src_load_datetime='LOAD_DATETIME',
            src_record_source='RECORD_SOURCE'
        ) }}
    
    Pattern: Only inserts rows where the hash diff has changed (SCD Type 2).
#}

WITH staging AS (

    SELECT
        {{ src_hash_key }}          AS HK,
        {{ src_hash_diff }}         AS HASH_DIFF,
        {% for col in src_payload_columns %}
        {{ col }},
        {% endfor %}
        {{ src_load_datetime }}     AS LOAD_DATETIME,
        {{ src_record_source }}     AS RECORD_SOURCE
    FROM {{ src_model }}

),

{% if is_incremental() %}
latest AS (

    SELECT HK, HASH_DIFF
    FROM (
        SELECT HK, HASH_DIFF,
            ROW_NUMBER() OVER (PARTITION BY HK ORDER BY LOAD_DATETIME DESC) AS RN
        FROM {{ this }}
    )
    WHERE RN = 1

),
{% endif %}

new_records AS (

    SELECT
        stg.HK,
        stg.HASH_DIFF,
        {% for col in src_payload_columns %}
        stg.{{ col }},
        {% endfor %}
        stg.LOAD_DATETIME,
        stg.RECORD_SOURCE
    FROM staging stg
    {% if is_incremental() %}
    LEFT JOIN latest lr ON stg.HK = lr.HK
    WHERE lr.HK IS NULL OR stg.HASH_DIFF != lr.HASH_DIFF
    {% endif %}

)

SELECT * FROM new_records

{% endmacro %}
