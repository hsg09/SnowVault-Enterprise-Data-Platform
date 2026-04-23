{% macro load_hub(hub_name, src_model, src_hash_key, src_business_key, src_load_datetime, src_record_source) %}
{#
    load_hub — Generic hub loading pattern.
    
    Usage:
        {{ load_hub(
            hub_name='hub_customer',
            src_model=ref('stg_ecommerce__customers'),
            src_hash_key='HK_CUSTOMER',
            src_business_key='CUSTOMER_ID',
            src_load_datetime='LOAD_DATETIME',
            src_record_source='RECORD_SOURCE'
        ) }}
    
    Pattern: Insert-only. Deduplicates by hash key, keeps earliest record.
#}

WITH staging AS (

    SELECT
        {{ src_hash_key }}          AS HK,
        {{ src_business_key }}      AS BK,
        {{ src_load_datetime }}     AS LOAD_DATETIME,
        {{ src_record_source }}     AS RECORD_SOURCE
    FROM {{ src_model }}

    {% if is_incremental() %}
    WHERE {{ src_load_datetime }} > (SELECT COALESCE(MAX(LOAD_DATETIME), '1900-01-01') FROM {{ this }})
    {% endif %}

),

deduplicated AS (

    SELECT
        HK, BK, LOAD_DATETIME, RECORD_SOURCE,
        ROW_NUMBER() OVER (PARTITION BY HK ORDER BY LOAD_DATETIME ASC) AS RN
    FROM staging

)

SELECT HK, BK, LOAD_DATETIME, RECORD_SOURCE
FROM deduplicated
WHERE RN = 1
{% if is_incremental() %}
  AND HK NOT IN (SELECT {{ src_hash_key }} FROM {{ this }})
{% endif %}

{% endmacro %}
