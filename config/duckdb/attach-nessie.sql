-- =============================================================================
-- attach-nessie.sql — DuckDB-Rezept fuer DE1 (Engine-Right-Sizing):
-- Nessies Iceberg-REST-Catalog anhaengen.
--
-- Deckt NUR die Katalog-/S3-Anbindung ab (das "fummelige" Stueck laut
-- Briefing). Die eigentliche Landing-Logik (Parquet lesen, Schrott-Saetze
-- filtern, typisieren, nach raw.taxi_duckdb schreiben) ist Aufgabe der
-- Gruppe.
--
-- Nutzung:
--   In DuckDB (CLI oder Python-Bindings, siehe unten) den Inhalt dieser
--   Datei ausfuehren, z.B.:
--     duckdb -init config/duckdb/attach-nessie.sql
--   oder in Python:
--     con.execute(open("config/duckdb/attach-nessie.sql").read())
--
-- Netzwerk-Kontext:
--   Die Hostnamen 'minio' und 'nessie' sind nur INNERHALB des Docker-Netzes
--   lakehouse-net aufloesbar (z.B. aus dem jupyter-Container heraus). Von
--   einem Host-Terminal aus 'minio' -> 'localhost', 'nessie' -> 'localhost'
--   ersetzen (Ports sind auf den Host gemappt, siehe .env).
--
-- Getestet gegen: Nessie 0.99.0, DuckDB 1.5.x, duckdb-iceberg Extension.
-- =============================================================================

INSTALL iceberg;
LOAD iceberg;
INSTALL httpfs;
LOAD httpfs;

-- S3-Zugangsdaten fuer MinIO (Werte aus .env: MINIO_ROOT_USER / MINIO_ROOT_PASSWORD).
CREATE OR REPLACE SECRET s3_minio (
    TYPE s3,
    ENDPOINT 'minio:9000',
    KEY_ID 'lakehouse',
    SECRET 'lakehouse123',
    REGION 'us-east-1',
    URL_STYLE 'path',
    USE_SSL false
);

-- Nessies Iceberg-REST-Catalog anhaengen (Warehouse-Name "warehouse", siehe
-- config/nessie/application.properties: nessie.catalog.default-warehouse).
--
-- WICHTIG — zwei Stolpersteine, die beim Aufbau dieses Rezepts aufgetreten
-- sind und die die Gruppe sich damit spart:
--
-- 1) AUTHORIZATION_TYPE 'none' ist noetig, weil die duckdb-iceberg Extension
--    sonst OAuth2-Client-Credentials erwartet (Default), die dieser
--    Demo-Stack nicht hat (Nessie laeuft ohne Auth).
--
-- 2) ACCESS_DELEGATION_MODE 'none' ist ZWINGEND. Der Default
--    'vended_credentials' laesst Nessie serverseitig eigene, temporaere
--    S3-Credentials fuer den Datenschreibpfad ausstellen — in diesem Setup
--    fuehrt das zu HTTP 403 gegen MinIO beim Schreiben der Parquet-Dateien
--    (Tabellen-Metadaten werden noch angelegt, der Commit schlaegt aber
--    fehl). Mit 'none' verwendet DuckDB durchgehend das oben angelegte
--    SECRET s3_minio statt der (hier fehlschlagenden) vended credentials.
ATTACH 'warehouse' AS lakehouse (
    TYPE ICEBERG,
    ENDPOINT 'http://nessie:19120/iceberg',
    AUTHORIZATION_TYPE 'none',
    ACCESS_DELEGATION_MODE 'none'
);

-- Smoke-Test (auskommentiert — zum manuellen Ausprobieren):
-- SHOW ALL TABLES;
--
-- Neue Tabelle mit expliziter Location im raw-Bucket anlegen (empfohlen,
-- damit Trino die Tabelle unter dem gewohnten raw.* Namespace-Pfad findet
-- und mit den Spark/Polars-Pendants konsistent bleibt):
--   CREATE TABLE lakehouse.raw.taxi_duckdb (...)
--   -- DuckDB-Iceberg unterstuetzt aktuell keine explizite LOCATION-Klausel
--   -- in CREATE TABLE; ohne Angabe landet die Tabelle unter der
--   -- Default-Warehouse-Location s3://warehouse/raw/taxi_duckdb/. Das ist
--   -- fuer den Engine-Vergleich unproblematisch (nur der Bucket-Name weicht
--   -- vom raw.*-Konvention der anderen Layer ab) — bei Bedarf per pyiceberg
--   -- (siehe scripts/pyiceberg_init.py) mit expliziter location= anlegen und
--   -- danach ueber DuckDB befuellen.
