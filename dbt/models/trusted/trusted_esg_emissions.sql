{{
    config(
        materialized='table',
        schema='trusted'
    )
}}

-- Trusted ESG Emissions: das fachlich freigegebene Endprodukt
-- 1:1-Uebernahme aus Curated, aber mit strikteren Tests in schema.yml
-- (insb. not_null auf scope_1_tco2e — in Curated waren NULLs erlaubt).
--
-- Promotion erfolgt nur nach gruenem Quality Gate (siehe AP-14).
-- Bewusst ohne WHERE-Filter, damit dbt test rot wird, wenn das Gate
-- umgangen wird — das ist der didaktische Hebel der Demo.

SELECT
    isin,
    reporting_year,
    source_system,
    scope_1_tco2e,
    scope_2_location_tco2e,
    scope_2_market_tco2e,
    scope_3_total_tco2e,
    verification,
    ingestion_id,
    ingestion_timestamp,
    source_file_hash
FROM {{ ref('curated_esg_emissions') }}
