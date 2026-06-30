{#
    dbt's default generate_schema_name macro joins custom schemas as
    "{{target.schema}}_{{custom_schema}}", which produces invalid table names
    like "ecommerce_lakehouse.silver".

    This override uses the custom schema as-is when given, otherwise falls back
    to target.schema. That keeps the "lakehouse.silver" three-part name correct.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
