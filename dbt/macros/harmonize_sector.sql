{% macro harmonize_sector(column_name) %}
    CASE
        WHEN {{ column_name }} IS NULL THEN NULL
        WHEN UPPER(TRIM({{ column_name }})) IN ('AUTOMOBILES', 'AUTOMOBILE') THEN 'Industrials'
        WHEN UPPER(TRIM({{ column_name }})) IN ('CHEMICALS', 'CHEMICAL') THEN 'Materials'
        WHEN UPPER(TRIM({{ column_name }})) IN ('PHARMACEUTICALS', 'PHARMA') THEN 'Health Care'
        WHEN UPPER(TRIM({{ column_name }})) IN ('INFORMATION TECHNOLOGY', 'IT') THEN 'Technology'
        WHEN UPPER(TRIM({{ column_name }})) IN ('FINANCIAL SERVICES', 'BANKING') THEN 'Financials'
        WHEN UPPER(TRIM({{ column_name }})) IN ('TELECOMMUNICATIONS', 'TELECOM') THEN 'Communication Services'
        ELSE TRIM({{ column_name }})
    END
{% endmacro %}
