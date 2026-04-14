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
