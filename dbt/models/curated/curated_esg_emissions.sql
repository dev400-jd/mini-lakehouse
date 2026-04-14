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
