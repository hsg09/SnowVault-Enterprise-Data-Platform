{% macro hash_key(columns) %}
{#
    hash_key — Generate a SHA-256 hash key from one or more business key columns.
    
    Usage:
        {{ hash_key(['CUSTOMER_ID']) }}
        {{ hash_key(['ORDER_ID', 'PRODUCT_ID']) }}
    
    Output: SHA2_BINARY encoded as VARCHAR(64) hex string.
    NULL handling: NULLs are coalesced to empty string before hashing.
    Separator: '||' between multiple columns to prevent collision.
#}
    SHA2(
        CONCAT_WS('||',
            {% for col in columns %}
            COALESCE(CAST({{ col }} AS VARCHAR), '')
            {%- if not loop.last %},{% endif %}
            {% endfor %}
        ), 256
    )
{% endmacro %}
