{% macro snowpipe_status(pipe_name) %}
{#
    snowpipe_status — Check the status of a Snowpipe and return recent history.
    Usage: {{ snowpipe_status('RAW_VAULT.ECOMMERCE.PIPE_CUSTOMERS') }}
#}
    SELECT
        PIPE_NAME,
        PIPE_STATUS,
        LAST_INGESTED_TIMESTAMP,
        LAST_INGESTED_FILE_PATH,
        NOTIFICATION_CHANNEL_INTEGRATION,
        ERROR_MSGS
    FROM TABLE(INFORMATION_SCHEMA.PIPE_STATUS('{{ pipe_name }}'))
{% endmacro %}

{% macro snowpipe_copy_history(pipe_name, hours=24) %}
{#
    Returns COPY_HISTORY for a specific Snowpipe over the last N hours.
#}
    SELECT
        FILE_NAME,
        STAGE_LOCATION,
        STATUS,
        ROW_COUNT,
        ROW_PARSED,
        FILE_SIZE,
        FIRST_ERROR_MESSAGE,
        LAST_LOAD_TIME
    FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => '{{ pipe_name }}',
        START_TIME => DATEADD('hour', -{{ hours }}, CURRENT_TIMESTAMP())
    ))
    ORDER BY LAST_LOAD_TIME DESC
{% endmacro %}
