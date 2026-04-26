-- TEMPORAER DEAKTIVIERT in AP-10:
-- Dieses Modell bezieht sich auf die deaktivierten Staging-Modelle
-- (stg_cdp_emissions, stg_nzdpu_emissions) und wird in AP-12
-- reaktiviert, sobald Staging neu gebaut ist.

{{ config(
    enabled=false,
    materialized='table'
) }}

-- Beide ESG-Quellen (CDP + NZDPU) vereinigt und dedupliziert
-- Bei Duplikaten (gleiche ISIN + gleicher Reporting Year) wird NZDPU bevorzugt
-- weil NZDPU detailliertere Daten liefert (LEI, Verification Status)

WITH combined AS (
    SELECT
        company_name,
        country,
        isin,
        sector,
        reporting_year,
        scope_1_tco2e,
        scope_2_tco2e,
        data_source,
        ROW_NUMBER() OVER (
            PARTITION BY isin, reporting_year
            ORDER BY CASE WHEN data_source = 'NZDPU' THEN 1 ELSE 2 END
        ) AS rn
    FROM (
        SELECT company_name, country, isin, sector, reporting_year,
               scope_1_tco2e, scope_2_tco2e, data_source
        FROM {{ ref('stg_cdp_emissions') }}

        UNION ALL

        SELECT company_name, country, isin, sector, reporting_year,
               scope_1_tco2e, scope_2_tco2e, data_source
        FROM {{ ref('stg_nzdpu_emissions') }}
    )
)

SELECT
    company_name,
    country,
    isin,
    sector,
    reporting_year,
    scope_1_tco2e,
    scope_2_tco2e,
    data_source
FROM combined
WHERE rn = 1
