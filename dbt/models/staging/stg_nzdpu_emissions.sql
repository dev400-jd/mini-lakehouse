{{
    config(
        materialized='table',
        schema='staging'
    )
}}

-- Staging-Modell fuer NZDPU-Emissionen
-- Doppelter UNNEST ueber data[] -> reporting_periods[]
-- Ergebnis: 90 flache Rows (30 Companies x 3 Jahre)
-- Provenance aus File-level Raw durchgereicht

SELECT
    -- Provenance
    r.ingestion_id,
    r.ingestion_timestamp,
    r.source_system,
    r.source_version,
    r.source_file_path,
    r.source_file_hash,

    -- Fachliche Felder Company-Level (1. UNNEST)
    json_extract_scalar(company, '$.company_id')               AS company_id,
    json_extract_scalar(company, '$.company_name')             AS company_name,
    json_extract_scalar(company, '$.isin')                     AS isin,
    json_extract_scalar(company, '$.lei')                      AS lei,
    json_extract_scalar(company, '$.country_of_incorporation') AS country,
    json_extract_scalar(company, '$.primary_sector')           AS primary_sector,

    -- Fachliche Felder Period-Level (2. UNNEST)
    CAST(json_extract_scalar(period, '$.reporting_year') AS INTEGER) AS reporting_year,
    json_extract_scalar(period, '$.reporting_framework')             AS reporting_framework,
    json_extract_scalar(period, '$.verification_status')             AS verification_status,

    -- Scope-Werte: NZDPU liefert sie als Dict {value, unit}
    CAST(json_extract_scalar(period, '$.scope_1.value') AS DOUBLE) AS scope_1_tco2e,
    json_extract_scalar(period, '$.scope_1.unit')                  AS scope_1_unit,

    CAST(json_extract_scalar(period, '$.scope_2_location_based.value') AS DOUBLE) AS scope_2_location_tco2e,
    CAST(json_extract_scalar(period, '$.scope_2_market_based.value')   AS DOUBLE) AS scope_2_market_tco2e,

    -- Scope 3 ist haeufig null; nur Total-Wert in Staging
    CAST(json_extract_scalar(period, '$.scope_3.total') AS DOUBLE) AS scope_3_total_tco2e

FROM {{ source('raw', 'nzdpu_emissions') }} r
CROSS JOIN UNNEST(
    CAST(json_extract(r.raw_payload, '$.data') AS ARRAY(JSON))
) AS u1(company)
CROSS JOIN UNNEST(
    CAST(json_extract(company, '$.reporting_periods') AS ARRAY(JSON))
) AS u2(period)
