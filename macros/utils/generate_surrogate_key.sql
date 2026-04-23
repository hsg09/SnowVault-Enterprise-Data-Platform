{% macro generate_surrogate_key(columns) %}
{#
    generate_surrogate_key — Wrapper for dbt_utils surrogate key.
    Provides a single point of change if we switch hashing algorithms.
#}
    {{ dbt_utils.generate_surrogate_key(columns) }}
{% endmacro %}

{% macro incremental_lookback(lookback_days=none) %}
{#
    incremental_lookback — Standard lookback filter for incremental models.
    Usage:
        WHERE LOAD_DATETIME >= {{ incremental_lookback() }}
#}
    {% set days = lookback_days or var('incremental_lookback_days', 3) %}
    DATEADD('day', -{{ days }}, CURRENT_DATE())
{% endmacro %}

{% macro safe_cast(column_name, target_type, default_value='NULL') %}
{#
    safe_cast — TRY_CAST with fallback to default value.
    Usage: {{ safe_cast('PRICE', 'NUMBER(18,4)', '0') }}
#}
    COALESCE(TRY_CAST({{ column_name }} AS {{ target_type }}), {{ default_value }})
{% endmacro %}
