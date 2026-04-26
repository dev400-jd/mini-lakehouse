{{
    config(
        materialized='table',
        schema='curated'
    )
}}

-- Curated Emissions: pro (ISIN, Jahr, Source) ein Record
-- Beide Quellen separat erhalten — Source-of-Truth-Entscheidung
-- erfolgt spaeter (Trusted oder Semantic Layer).
-- Records ohne ISIN werden ausgefiltert (fachlich nicht verwertbar).

WITH nzdpu_emissions AS (
    SELECT
        {{ standardize_isin('isin') }}                  AS isin,
        reporting_year                                  AS reporting_year,
        'nzdpu'                                         AS source_system,
        {{ m_to_decimal('scope_1_tco2e') }}             AS scope_1_tco2e,
        {{ m_to_decimal('scope_2_location_tco2e') }}    AS scope_2_location_tco2e,
        {{ m_to_decimal('scope_2_market_tco2e') }}      AS scope_2_market_tco2e,
        {{ m_to_decimal('scope_3_total_tco2e') }}       AS scope_3_total_tco2e,
        verification_status                             AS verification,
        ingestion_id                                    AS ingestion_id,
        ingestion_timestamp                             AS ingestion_timestamp,
        source_file_hash                                AS source_file_hash
    FROM {{ ref('stg_nzdpu_emissions') }}
    WHERE isin IS NOT NULL
),

cdp_emissions AS (
    SELECT
        {{ standardize_isin('isin') }}                  AS isin,
        reporting_year                                  AS reporting_year,
        'cdp'                                           AS source_system,
        {{ m_to_decimal('scope_1_tco2e') }}             AS scope_1_tco2e,
        {{ m_to_decimal('scope_2_location_tco2e') }}    AS scope_2_location_tco2e,
        {{ m_to_decimal('scope_2_market_tco2e') }}      AS scope_2_market_tco2e,
        {{ m_to_decimal('scope_3_total_tco2e') }}       AS scope_3_total_tco2e,
        data_verification                               AS verification,
        ingestion_id                                    AS ingestion_id,
        ingestion_timestamp                             AS ingestion_timestamp,
        source_file_hash                                AS source_file_hash
    FROM {{ ref('stg_cdp_emissions') }}
    WHERE isin IS NOT NULL
)

SELECT * FROM nzdpu_emissions
UNION ALL
SELECT * FROM cdp_emissions
