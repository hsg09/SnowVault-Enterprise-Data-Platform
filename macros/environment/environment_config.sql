{% macro environment_config() %}
{#
    environment_config — Return current environment configuration.
    Useful for conditional logic based on deployment target.
#}
    {% set env = var('environment', 'dev') %}
    {% do return(env) %}
{% endmacro %}

{% macro is_production() %}
    {{ return(var('environment', 'dev') == 'prod') }}
{% endmacro %}

{% macro multi_region(primary_database, secondary_suffix='_REPLICA') %}
{#
    multi_region — Reference the correct database for the current region.
    In multi-region setup, secondary regions use replica databases.
#}
    {% if var('is_secondary_region', false) %}
        {{ primary_database }}{{ secondary_suffix }}
    {% else %}
        {{ primary_database }}
    {% endif %}
{% endmacro %}
