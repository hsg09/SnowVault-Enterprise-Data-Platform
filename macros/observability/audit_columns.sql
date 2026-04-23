{% macro audit_columns() %}
{#
    audit_columns — Add standard audit columns to any model.
    Usage: SELECT *, {{ audit_columns() }} FROM my_table
#}
    CURRENT_TIMESTAMP()     AS _DW_LOADED_AT,
    '{{ this.schema }}'     AS _DW_SCHEMA,
    '{{ this.name }}'       AS _DW_MODEL_NAME,
    '{{ invocation_id }}'   AS _DW_INVOCATION_ID
{% endmacro %}

{% macro logging(message, severity='INFO') %}
{#
    logging — Log a message to the dbt console output.
    Usage: {{ logging('Starting customer hub load', 'INFO') }}
#}
    {{ log(severity ~ ' | ' ~ this.name ~ ' | ' ~ message, info=true) }}
{% endmacro %}

{% macro data_quality(model_name, column_name, check_type, threshold=0) %}
{#
    data_quality — Inline DQ check that returns a boolean.
    Usage: {{ data_quality('hub_customer', 'HK_CUSTOMER', 'null_check') }}
#}
    {% if check_type == 'null_check' %}
    (SELECT COUNT(*) FROM {{ ref(model_name) }} WHERE {{ column_name }} IS NULL) <= {{ threshold }}
    {% elif check_type == 'unique_check' %}
    (SELECT COUNT(*) - COUNT(DISTINCT {{ column_name }}) FROM {{ ref(model_name) }}) <= {{ threshold }}
    {% elif check_type == 'row_count' %}
    (SELECT COUNT(*) FROM {{ ref(model_name) }}) > {{ threshold }}
    {% endif %}
{% endmacro %}

{% macro sla_monitor(model_name, max_age_hours=24) %}
{#
    sla_monitor — Check if a model's data is within SLA freshness.
    Returns TRUE if the most recent record is within max_age_hours.
#}
    SELECT
        CASE
            WHEN DATEDIFF('hour', MAX(LOAD_DATETIME), CURRENT_TIMESTAMP()) <= {{ max_age_hours }}
                THEN 'WITHIN_SLA'
            ELSE 'SLA_BREACH'
        END AS SLA_STATUS,
        MAX(LOAD_DATETIME) AS LATEST_RECORD,
        DATEDIFF('hour', MAX(LOAD_DATETIME), CURRENT_TIMESTAMP()) AS HOURS_SINCE_UPDATE,
        {{ max_age_hours }} AS SLA_THRESHOLD_HOURS
    FROM {{ ref(model_name) }}
{% endmacro %}
