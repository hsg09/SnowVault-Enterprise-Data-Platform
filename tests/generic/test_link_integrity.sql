{% test link_integrity(model, link_hash_key, fk_columns, hub_models) %}
{#
    test_link_integrity — Verify every FK in a link table exists in its parent hub.
    
    Usage:
        tests:
          - link_integrity:
              link_hash_key: HK_LINK_CUSTOMER_ORDER
              fk_columns: ['HK_CUSTOMER', 'HK_ORDER']
              hub_models: [ref('hub_customer'), ref('hub_order')]
#}

{% for i in range(fk_columns | length) %}
SELECT
    '{{ fk_columns[i] }}' AS FK_COLUMN,
    {{ fk_columns[i] }}   AS FK_VALUE
FROM {{ model }}
WHERE {{ fk_columns[i] }} NOT IN (
    SELECT {{ fk_columns[i] }} FROM {{ hub_models[i] }}
)
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %}

{% endtest %}
