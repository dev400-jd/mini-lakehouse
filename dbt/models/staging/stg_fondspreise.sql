-- Staging-Modell fuer Fondspreise
-- Expandiert File-level Raw (ein Row pro Datei) in einzelne Fonds-Tag-Records
-- Cast-Logik: JSON-Strings -> Typen, harmonisierte Spaltennamen
-- Provenance-Spalten aus Raw durchgereicht

SELECT
    -- Provenance
    r.ingestion_id,
    r.ingestion_timestamp,
    r.source_system,
    r.source_version,
    r.source_file_path,
    r.source_file_hash,

    -- Fachliche Felder
    json_extract_scalar(rec, '$.isin')                          AS isin,
    json_extract_scalar(rec, '$.fund_name')                     AS fund_name,
    CAST(json_extract_scalar(rec, '$.business_date') AS DATE)   AS business_date,
    CAST(json_extract_scalar(rec, '$.nav') AS DOUBLE)           AS nav,
    json_extract_scalar(rec, '$.currency')                      AS currency

FROM {{ source('raw', 'fondspreise') }} r
CROSS JOIN UNNEST(
    CAST(json_extract(r.raw_payload, '$.records') AS ARRAY(JSON))
) AS u(rec)
