{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set default_schema = target.schema -%}

    {%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}

    {%- elif node.path.startswith('bronze/') -%}
        {{ default_schema }}_bronze

    {%- elif node.path.startswith('silver/') -%}
        {{ default_schema }}_silver

    {%- elif node.path.startswith('gold/') -%}
        {{ default_schema }}_gold

    {%- else -%}
        {{ default_schema }}

    {%- endif -%}

{%- endmacro %}