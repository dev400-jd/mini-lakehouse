{{
    config(
        materialized='table',
        schema='curated'
    )
}}

-- Curated Mitarbeiterzahlen: pro (ISIN, Jahr) ein Record
-- ISIN harmonisiert, Records ohne ISIN oder Jahr ausgefiltert,
-- Duplikate (falls mehrere Ingestion-Runs dieselbe Kombination liefern)
-- ueber den juengsten Ingestion-Timestamp aufgeloest.

WITH cleaned AS (
    SELECT
        {{ standardize_isin('isin') }} AS isin,
        NULLIF(TRIM(unternehmen), '')  AS unternehmen,
        jahr,
        mitarbeiterzahl,
        NULLIF(TRIM(region), '')       AS region,
        ingestion_timestamp
    FROM {{ ref('stg_mitarbeiterzahlen') }}
    WHERE isin IS NOT NULL
      AND jahr IS NOT NULL
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY isin, jahr
            ORDER BY ingestion_timestamp DESC
        ) AS recency_rank
    FROM cleaned
)

SELECT
    isin,
    unternehmen,
    jahr,
    mitarbeiterzahl,
    region
FROM ranked
WHERE recency_rank = 1
