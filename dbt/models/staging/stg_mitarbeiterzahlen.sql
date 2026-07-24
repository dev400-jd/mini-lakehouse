{{
    config(
        materialized='table',
        schema='staging'
    )
}}

-- Staging-Modell fuer Mitarbeiterzahlen (CSV-Quelle)
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
    FROM {{ source('raw', 'mitarbeiterzahlen') }} r
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

    -- 5 CSV-Spalten in Header-Reihenfolge
    NULLIF(split_part(line, ',', 1), '')                       AS isin,
    NULLIF(split_part(line, ',', 2), '')                       AS unternehmen,
    TRY_CAST(NULLIF(split_part(line, ',', 3), '') AS INTEGER)  AS jahr,
    TRY_CAST(NULLIF(split_part(line, ',', 4), '') AS INTEGER)  AS mitarbeiterzahl,
    NULLIF(split_part(line, ',', 5), '')                       AS region

FROM csv_lines
