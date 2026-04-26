{% macro to_decimal(column_name, precision=18, scale=3) %}
    CAST({{ column_name }} AS DECIMAL({{ precision }}, {{ scale }}))
{% endmacro %}
