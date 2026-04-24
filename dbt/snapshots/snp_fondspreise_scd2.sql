{% snapshot snp_fondspreise_scd2 %}

{{
    config(
        target_schema='curated',
        target_database='nessie',
        unique_key=['isin', 'business_date'],
        strategy='check',
        check_cols=['nav', 'currency'],
        invalidate_hard_deletes=False
    )
}}

-- SCD2-Historisierung der Fondspreise.
-- Listen-unique_key (isin, business_date) wird von dbt-trino nativ
-- unterstuetzt. Aenderungen an nav oder currency erzeugen eine neue
-- SCD2-Version.

SELECT
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
