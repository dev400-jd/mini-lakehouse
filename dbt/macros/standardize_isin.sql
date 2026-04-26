{% macro standardize_isin(column_name) %}
    NULLIF(UPPER(TRIM({{ column_name }})), '')
{% endmacro %}
