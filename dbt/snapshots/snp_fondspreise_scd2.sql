{% snapshot snp_fondspreise_scd2 %}

{{
    config(
        target_schema='curated',
        target_database='nessie',
        unique_key='scd_key',
        strategy='check',
        check_cols=['nav', 'currency'],
        invalidate_hard_deletes=False,
        file_format='iceberg',
    )
}}

-- SCD2-Historisierung der Fondspreise.
-- scd_key als Surrogate Key verhindert Probleme mit Expression-unique_key in dbt-trino.
-- Trennzeichen '|' schliesst Kollisionen aus (ISINs enthalten keine Pipes).
-- Aenderungen an nav oder currency erzeugen eine neue SCD2-Version.

SELECT
    isin || '|' || CAST(business_date AS VARCHAR) AS scd_key,

    isin,
    business_date,

    nav,
    currency,
    fund_name,

    ingestion_id,
    ingestion_timestamp,
    source_system,
    source_version,
    source_file_hash

FROM {{ ref('stg_fondspreise') }}

{% endsnapshot %}
