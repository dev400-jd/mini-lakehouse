-- TEMPORAER DEAKTIVIERT in AP-10:
-- Diese Staging-Logik bezieht sich auf das alte Source-Schema
-- (strukturierte Spalten via Spark-Ingestion aus JSON). Mit AP-10
-- wurde die Source auf File-level Raw umgestellt (raw_payload als
-- String). AP-11 wird dieses Modell auf doppelten JSON-UNNEST
-- (data[] x reporting_periods[]) umschreiben.

{{ config(
    enabled=false,
    materialized='table'
) }}

-- NZDPU-Daten normalisieren auf das gleiche Schema wie CDP
-- Felder sind bereits typisiert (Spark-Ingestion aus JSON)

SELECT
    company_name,
    country,
    isin,
    lei,
    sector,
    CAST(reporting_year AS INTEGER)         AS reporting_year,
    CAST(scope_1_tco2e AS DOUBLE)           AS scope_1_tco2e,
    CAST(scope_2_location_tco2e AS DOUBLE)  AS scope_2_tco2e,
    verification_status,
    'NZDPU'                                 AS data_source
FROM {{ source('raw', 'nzdpu_emissions') }}
WHERE company_name IS NOT NULL
