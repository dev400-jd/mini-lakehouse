{{
    config(
        materialized='table',
        schema='staging'
    )
}}

-- Staging-Modell fuer CDP-Emissionen (CSV-Quelle)
-- CSV-Expand mit split + WITH ORDINALITY, Header uebersprungen
-- Pro Datenzeile: Spalten via split_part, leere Werte als NULL,
-- Numerik mit TRY_CAST defensiv konvertiert.
-- Provenance aus File-level Raw durchgereicht.

WITH csv_lines AS (
    SELECT
        r.ingestion_id,
        r.ingestion_timestamp,
        r.source_system,
        r.source_version,
        r.source_file_path,
        r.source_file_hash,
        sequence_index AS row_number,
        line
    FROM {{ source('raw', 'cdp_emissions') }} r
    CROSS JOIN UNNEST(
        split(r.raw_payload, chr(10))
    ) WITH ORDINALITY AS u(line, sequence_index)
    WHERE sequence_index > 1
      AND length(trim(line)) > 0
)

SELECT
    -- Provenance
    ingestion_id,
    ingestion_timestamp,
    source_system,
    source_version,
    source_file_path,
    source_file_hash,
    row_number,

    -- 15 CSV-Spalten in Header-Reihenfolge
    NULLIF(split_part(line, ',', 1),  '')                      AS account_number,
    NULLIF(split_part(line, ',', 2),  '')                      AS organization,
    NULLIF(split_part(line, ',', 3),  '')                      AS primary_sector,
    NULLIF(split_part(line, ',', 4),  '')                      AS primary_industry,
    NULLIF(split_part(line, ',', 5),  '')                      AS country,
    NULLIF(split_part(line, ',', 6),  '')                      AS isin,
    TRY_CAST(NULLIF(split_part(line, ',', 7),  '') AS INTEGER) AS reporting_year,
    TRY_CAST(NULLIF(split_part(line, ',', 8),  '') AS DOUBLE)  AS scope_1_tco2e,
    TRY_CAST(NULLIF(split_part(line, ',', 9),  '') AS DOUBLE)  AS scope_2_location_tco2e,
    TRY_CAST(NULLIF(split_part(line, ',', 10), '') AS DOUBLE)  AS scope_2_market_tco2e,
    TRY_CAST(NULLIF(split_part(line, ',', 11), '') AS DOUBLE)  AS scope_3_total_tco2e,
    NULLIF(split_part(line, ',', 12), '')                      AS emission_unit,
    NULLIF(split_part(line, ',', 13), '')                      AS data_verification,
    NULLIF(split_part(line, ',', 14), '')                      AS cdp_score,
    NULLIF(split_part(line, ',', 15), '')                      AS public_disclosure

FROM csv_lines
