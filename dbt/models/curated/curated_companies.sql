{{
    config(
        materialized='table',
        schema='curated'
    )
}}

-- Curated Stammdaten: pro ISIN ein Record
-- UNION beider Quellen (NZDPU + CDP), Distinct ueber ISIN
-- Bei Konflikten gewinnt NZDPU (LEI ist dort vorhanden)

WITH nzdpu_companies AS (
    SELECT DISTINCT
        {{ standardize_isin('isin') }}              AS isin,
        company_name                                AS company_name,
        lei                                         AS lei,
        country                                     AS country,
        {{ harmonize_sector('primary_sector') }}    AS primary_sector,
        'nzdpu'                                     AS source_system
    FROM {{ ref('stg_nzdpu_emissions') }}
    WHERE isin IS NOT NULL
),

cdp_companies AS (
    SELECT DISTINCT
        {{ standardize_isin('isin') }}              AS isin,
        organization                                AS company_name,
        CAST(NULL AS VARCHAR)                       AS lei,
        country                                     AS country,
        {{ harmonize_sector('primary_sector') }}    AS primary_sector,
        'cdp'                                       AS source_system
    FROM {{ ref('stg_cdp_emissions') }}
    WHERE isin IS NOT NULL
),

unioned AS (
    SELECT * FROM nzdpu_companies
    UNION ALL
    SELECT * FROM cdp_companies
),

ranked AS (
    SELECT
        isin,
        company_name,
        lei,
        country,
        primary_sector,
        source_system,
        ROW_NUMBER() OVER (
            PARTITION BY isin
            ORDER BY CASE WHEN source_system = 'nzdpu' THEN 1 ELSE 2 END
        ) AS source_priority
    FROM unioned
)

SELECT
    isin,
    company_name,
    lei,
    country,
    primary_sector,
    source_system AS preferred_source
FROM ranked
WHERE source_priority = 1
