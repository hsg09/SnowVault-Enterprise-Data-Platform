{% macro load_link(src_model, src_hash_key, src_fk_columns, src_load_datetime, src_record_source) %}
{#
    load_link — Generic link loading pattern.
    
    Usage:
        {{ load_link(
            src_model=ref('stg_ecommerce__orders'),
            src_hash_key='HK_LINK_CUSTOMER_ORDER',
            src_fk_columns=['HK_CUSTOMER', 'HK_ORDER'],
            src_load_datetime='LOAD_DATETIME',
            src_record_source='RECORD_SOURCE'
        ) }}
#}

WITH staging AS (

    SELECT
        {{ src_hash_key }}          AS HK_LINK,
        {% for fk in src_fk_columns %}
        {{ fk }},
        {% endfor %}
        {{ src_load_datetime }}     AS LOAD_DATETIME,
        {{ src_record_source }}     AS RECORD_SOURCE
    FROM {{ src_model }}

    {% if is_incremental() %}
    WHERE {{ src_load_datetime }} > (SELECT COALESCE(MAX(LOAD_DATETIME), '1900-01-01') FROM {{ this }})
    {% endif %}

),

deduplicated AS (

    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY HK_LINK ORDER BY LOAD_DATETIME ASC) AS RN
    FROM staging

)

SELECT
    HK_LINK,
    {% for fk in src_fk_columns %}
    {{ fk }},
    {% endfor %}
    LOAD_DATETIME,
    RECORD_SOURCE
FROM deduplicated
WHERE RN = 1
{% if is_incremental() %}
  AND HK_LINK NOT IN (SELECT {{ src_hash_key }} FROM {{ this }})
{% endif %}

{% endmacro %}
