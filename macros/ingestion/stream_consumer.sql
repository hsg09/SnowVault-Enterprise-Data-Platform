{% macro stream_consumer(stream_name) %}
{#
    stream_consumer — Check if a stream has data ready for consumption.
    Returns TRUE if there are pending CDC records.
    Usage: {{ stream_consumer('RAW_VAULT.ECOMMERCE.STREAM_CUSTOMERS') }}
#}
    SELECT SYSTEM$STREAM_HAS_DATA('{{ stream_name }}') AS HAS_DATA
{% endmacro %}
