{% macro load_bridge(link_model, link_hash_key, link_fk_columns, eff_sat_model=none) %}
{#
    load_bridge — Generate a Bridge table for link path walking.
    
    Usage:
        {{ load_bridge(
            link_model=ref('link_customer_order'),
            link_hash_key='HK_LINK_CUSTOMER_ORDER',
            link_fk_columns=['HK_CUSTOMER', 'HK_ORDER'],
            eff_sat_model=ref('eff_sat_customer_order')
        ) }}
    
    Pattern: Joins link with optional effectivity satellite for validity filtering.
#}

SELECT
    lnk.{{ link_hash_key }},
    {% for fk in link_fk_columns %}
    lnk.{{ fk }},
    {% endfor %}
    lnk.LOAD_DATETIME   AS LINK_LOAD_DATETIME,
    lnk.RECORD_SOURCE   AS LINK_RECORD_SOURCE
    {% if eff_sat_model %}
    , eff.IS_ACTIVE
    , eff.EFFECTIVE_FROM
    , eff.EFFECTIVE_TO
    {% endif %}

FROM {{ link_model }} lnk

{% if eff_sat_model %}
LEFT JOIN (
    SELECT *
    FROM {{ eff_sat_model }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {{ link_hash_key }}
        ORDER BY LOAD_DATETIME DESC
    ) = 1
) eff
    ON lnk.{{ link_hash_key }} = eff.{{ link_hash_key }}
{% endif %}

{% endmacro %}
