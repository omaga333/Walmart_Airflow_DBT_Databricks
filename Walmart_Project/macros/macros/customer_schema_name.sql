{% macro generate_schema_name(customer_schema_name, node) %}

    {% set default_schema = target.schema %}

    {% if customer_schema_name is none %}
        {{ default_schema }}
    {% else %}
        {{ customer_schema_name | trim }}
    {% endif %}

{% endmacro %}