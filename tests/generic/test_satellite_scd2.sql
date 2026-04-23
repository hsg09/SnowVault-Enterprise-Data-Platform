{% test satellite_scd2(model, hash_key, hash_diff) %}
{#
    test_satellite_scd2 — Verify satellites have no duplicate (hash_key, hash_diff)
    consecutive records. If two consecutive records have the same hash_diff,
    the change detection logic failed.
#}

WITH consecutive_check AS (

    SELECT
        {{ hash_key }},
        {{ hash_diff }},
        LOAD_DATETIME,
        LAG({{ hash_diff }}) OVER (
            PARTITION BY {{ hash_key }}
            ORDER BY LOAD_DATETIME
        ) AS PREV_HASH_DIFF
    FROM {{ model }}

)

SELECT *
FROM consecutive_check
WHERE {{ hash_diff }} = PREV_HASH_DIFF

{% endtest %}
