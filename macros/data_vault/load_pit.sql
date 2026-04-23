{% macro load_pit(hub_model, hub_hash_key, satellite_configs) %}
{#
    load_pit — Generate a Point-In-Time table for a hub and its satellites.
    
    Usage:
        {{ load_pit(
            hub_model=ref('hub_customer'),
            hub_hash_key='HK_CUSTOMER',
            satellite_configs=[
                {'model': ref('sat_customer_details'), 'alias': 'DETAILS'},
                {'model': ref('sat_customer_demographics'), 'alias': 'DEMOGRAPHICS'}
            ]
        ) }}
    
    Pattern: For each hub entity at each point in time, find the most recent
    satellite record loaded on or before that timestamp.
#}

WITH hub AS (
    SELECT DISTINCT {{ hub_hash_key }}, LOAD_DATETIME AS PIT_LOAD_DATETIME
    FROM {{ hub_model }}
    {% for sat in satellite_configs %}
    UNION
    SELECT DISTINCT {{ hub_hash_key }}, LOAD_DATETIME AS PIT_LOAD_DATETIME
    FROM {{ sat.model }}
    {% endfor %}
)

SELECT
    hub.{{ hub_hash_key }},
    hub.PIT_LOAD_DATETIME
    {% for sat in satellite_configs %}
    , COALESCE({{ sat.alias }}.LOAD_DATETIME, CAST('1900-01-01' AS TIMESTAMP_NTZ))
        AS SAT_{{ sat.alias }}_LDTS
    {% endfor %}
FROM hub
{% for sat in satellite_configs %}
LEFT JOIN LATERAL (
    SELECT LOAD_DATETIME
    FROM {{ sat.model }}
    WHERE {{ hub_hash_key }} = hub.{{ hub_hash_key }}
      AND LOAD_DATETIME <= hub.PIT_LOAD_DATETIME
    ORDER BY LOAD_DATETIME DESC
    LIMIT 1
) {{ sat.alias }}
{% endfor %}

{% endmacro %}
