{% test hub_integrity(model, hub_hash_key, hub_model) %}
{#
    test_hub_integrity — Verify every hash key in a model exists in its parent hub.
    
    Usage in schema.yml:
        tests:
          - hub_integrity:
              hub_hash_key: HK_CUSTOMER
              hub_model: ref('hub_customer')
#}

SELECT
    {{ hub_hash_key }}
FROM {{ model }}
WHERE {{ hub_hash_key }} NOT IN (
    SELECT {{ hub_hash_key }} FROM {{ hub_model }}
)
AND {{ hub_hash_key }} != '{{ var("ghost_record_hash_key") }}'

{% endtest %}
