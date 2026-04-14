-- CDP-Daten bereinigen: Strings -> typisiert, NULLs behandeln, normalisieren
-- Spaltennamen mit Leerzeichen/Sonderzeichen muessen gequotet werden (Iceberg via Trino)

SELECT
    TRIM("organization")                                        AS company_name,
    TRIM("country")                                             AS country,
    NULLIF(TRIM("isin"), '')                                    AS isin,
    NULLIF(TRIM("primary sector"), '')                          AS sector,
    TRY_CAST("reporting year" AS INTEGER)                       AS reporting_year,
    TRY_CAST("scope 1 (metric tons co2e)" AS DOUBLE)            AS scope_1_tco2e,
    TRY_CAST("scope 2 location-based (metric tons co2e)"
             AS DOUBLE)                                         AS scope_2_tco2e,
    "data verification"                                         AS verification_status,
    "cdp score"                                                 AS cdp_score,
    'CDP'                                                       AS data_source
FROM {{ source('raw', 'cdp_emissions') }}
WHERE "organization" IS NOT NULL
  AND TRIM("organization") <> ''
