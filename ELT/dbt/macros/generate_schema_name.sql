{#
  dbt's default behavior concatenates target.schema + custom_schema
  (e.g. "elt_raw_ecommerce_elt_marts_ecommerce"). We want +schema to name
  the dataset exactly, since elt_marts_ecommerce already exists as its own
  BigQuery dataset (see ELT/scripts/setup_datasets.py) — not a suffix.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
