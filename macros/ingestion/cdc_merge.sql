{% macro cdc_merge(target_table, source_stream, business_key, columns) %}
{#
    cdc_merge — Merge CDC stream changes into a target table.
    Handles INSERT, UPDATE, and DELETE operations from Snowflake Streams.
    
    Usage:
        {{ cdc_merge(
            target_table='RAW_VAULT.ECOMMERCE.RAW_CUSTOMERS',
            source_stream='RAW_VAULT.ECOMMERCE.STREAM_CUSTOMERS',
            business_key='CUSTOMER_ID',
            columns=['FIRST_NAME', 'LAST_NAME', 'EMAIL', 'PHONE']
        ) }}
#}
    MERGE INTO {{ target_table }} tgt
    USING (
        SELECT *
        FROM {{ source_stream }}
        WHERE METADATA$ACTION = 'INSERT'
    ) src
    ON tgt.{{ business_key }} = src.{{ business_key }}

    WHEN MATCHED AND src.METADATA$ISUPDATE = TRUE THEN
        UPDATE SET
            {% for col in columns %}
            tgt.{{ col }} = src.{{ col }}
            {%- if not loop.last %},{% endif %}
            {% endfor %}

    WHEN NOT MATCHED THEN
        INSERT ({{ business_key }}, {{ columns | join(', ') }})
        VALUES (src.{{ business_key }}, 
            {% for col in columns %}
            src.{{ col }}
            {%- if not loop.last %},{% endif %}
            {% endfor %}
        )
{% endmacro %}
