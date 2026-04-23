{% macro dynamic_masking(column_name, pii_type='NONE') %}
{#
    dynamic_masking — Apply role-based masking inline.
    For use in models where table-level policies can't be applied.
    
    Usage: {{ dynamic_masking('EMAIL', 'EMAIL') }}
#}
    {% if pii_type == 'EMAIL' %}
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'PLATFORM_ADMIN') THEN {{ column_name }}
        ELSE REGEXP_REPLACE({{ column_name }}, '^(.{2})(.*)(@.*)$', '\\1***\\3')
    END
    {% elif pii_type == 'PHONE' %}
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'PLATFORM_ADMIN') THEN {{ column_name }}
        ELSE CONCAT('***-***-', RIGHT({{ column_name }}, 4))
    END
    {% elif pii_type == 'NAME' %}
    CASE
        WHEN CURRENT_ROLE() IN ('DATA_ENGINEER', 'PLATFORM_ADMIN') THEN {{ column_name }}
        ELSE CONCAT(LEFT({{ column_name }}, 1), '****')
    END
    {% else %}
    {{ column_name }}
    {% endif %}
{% endmacro %}

{% macro grant_management(role_name, database_name, privilege='SELECT') %}
{#
    grant_management — Generate GRANT statements for a role on a database.
#}
    GRANT {{ privilege }} ON ALL TABLES IN DATABASE {{ database_name }} TO ROLE {{ role_name }};
    GRANT {{ privilege }} ON FUTURE TABLES IN DATABASE {{ database_name }} TO ROLE {{ role_name }};
{% endmacro %}

{% macro tag_management(table_name, tag_name, tag_value) %}
{#
    tag_management — Apply a governance tag to a table.
#}
    ALTER TABLE {{ table_name }} SET TAG RAW_VAULT.GOVERNANCE.{{ tag_name }} = '{{ tag_value }}';
{% endmacro %}

{% macro row_access_policy(policy_name, column_name, allowed_roles) %}
{#
    row_access_policy — Generate a row access policy based on role membership.
#}
    CREATE OR REPLACE ROW ACCESS POLICY {{ policy_name }}
    AS ({{ column_name }} VARCHAR) RETURNS BOOLEAN ->
        CURRENT_ROLE() IN (
            {% for role in allowed_roles %}
            '{{ role }}'{% if not loop.last %},{% endif %}
            {% endfor %}
        )
    ;
{% endmacro %}
