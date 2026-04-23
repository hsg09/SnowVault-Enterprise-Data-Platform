{% macro hash_diff(columns) %}
{#
    hash_diff — Generate a SHA-256 hash diff for satellite change detection.
    
    Usage:
        {{ hash_diff(['FIRST_NAME', 'LAST_NAME', 'EMAIL']) }}
    
    This hash is compared against the latest satellite record to determine
    if any descriptive attribute has changed. Only rows with a different
    hash diff are inserted (SCD Type 2 pattern).
    
    NULL handling: NULLs are coalesced to '^^' (sentinel) to distinguish
    from empty strings.
#}
    SHA2(
        CONCAT_WS('||',
            {% for col in columns %}
            COALESCE(CAST({{ col }} AS VARCHAR), '^^')
            {%- if not loop.last %},{% endif %}
            {% endfor %}
        ), 256
    )
{% endmacro %}
