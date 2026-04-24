# Demo 1 — Curated: snp_fondspreise_scd2

## Was macht der Snapshot?

`snp_fondspreise_scd2` historisiert Fondspreis-Änderungen nach SCD2-Prinzip.

Bei jedem `dbt snapshot`-Run wird `stg_fondspreise` mit dem aktuellen
Snapshot-Stand verglichen. Ändert sich `nav` oder `currency` für eine
`(isin, business_date)`-Kombination:

- Die alte Version bekommt `dbt_valid_to` gesetzt (Zeitpunkt des Runs)
- Eine neue Version wird eingefügt mit `dbt_valid_to = NULL` (= aktuell gültig)

Unveränderte Zeilen bleiben unberührt.

## dbt snapshot vs. dbt run

| | `dbt run` | `dbt snapshot` |
|---|---|---|
| Ziel | Modelle (Views/Tables) | Snapshot-Tabellen |
| Verhalten | Tabelle wird komplett neu gebaut | Nur geänderte Zeilen werden aktualisiert |
| SCD2 | Nein | Ja — alte Versionen bleiben erhalten |
| Aufruf | `dbt run --select <modell>` | `dbt snapshot --select <snapshot>` |

**Wichtig:** Immer zuerst Staging refreshen, dann Snapshot:
```bash
dbt run --select stg_fondspreise
dbt snapshot --select snp_fondspreise_scd2
```
Sonst arbeitet der Snapshot mit veralteten Staging-Daten.

---

## Ausführung (PowerShell)

**Packages installieren** (einmalig):
```powershell
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt deps"
```

**Staging refreshen + Snapshot ausführen:**
```powershell
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt run --select stg_fondspreise"
```
```powershell
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt snapshot --select snp_fondspreise_scd2"
```

**Tests ausführen:**
```powershell
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select snp_fondspreise_scd2"
```

---

## SCD2-Historie verifizieren

**Alle Versionen einer ISIN anzeigen:**
```sql
SELECT isin, business_date, nav, source_version, dbt_valid_from, dbt_valid_to FROM nessie.curated.snp_fondspreise_scd2 WHERE isin = 'DE000A1JX0V2' AND business_date = DATE '2026-04-20' ORDER BY dbt_valid_from;
```

Erwartet nach Load 1 + Load 2 + zwei Snapshot-Runs:
- Zeile 1: `nav=127.49`, `source_version='v1'`, `dbt_valid_to` gesetzt
- Zeile 2: `nav=126.81`, `source_version='v1-correction'`, `dbt_valid_to=NULL`

**Anzahl aktuell gültiger Versionen:**
```sql
SELECT COUNT(*) FROM nessie.curated.snp_fondspreise_scd2 WHERE dbt_valid_to IS NULL;
```

**Gesamte Versionszahl:**
```sql
SELECT COUNT(*) FROM nessie.curated.snp_fondspreise_scd2;
```

---

## Welcher Preis galt am 20.04.?

Aktuell gültige Version (nach Korrektur):
```sql
SELECT nav, source_version, dbt_valid_from FROM nessie.curated.snp_fondspreise_scd2 WHERE isin = 'DE000A1JX0V2' AND business_date = DATE '2026-04-20' AND dbt_valid_to IS NULL;
```

Originalwert vor Korrektur (historisch):
```sql
SELECT nav, source_version, dbt_valid_from, dbt_valid_to FROM nessie.curated.snp_fondspreise_scd2 WHERE isin = 'DE000A1JX0V2' AND business_date = DATE '2026-04-20' ORDER BY dbt_valid_from;
```

---

## Unique Key

`(isin, business_date)` als Listen-`unique_key` — dbt-trino-nativer Standard,
kein Surrogate-Key nötig. dbt erkennt Änderungen über einen Hash der
`check_cols` (`nav`, `currency`).

## dbt-Meta-Spalten im Snapshot

| Spalte | Bedeutung |
|--------|-----------|
| `dbt_scd_id` | Eindeutige ID pro Version (Hash) |
| `dbt_updated_at` | Zeitpunkt der letzten Änderung |
| `dbt_valid_from` | Gültigkeits-Start dieser Version |
| `dbt_valid_to` | Gültigkeits-Ende — `NULL` = aktuell gültig |

## Reset vor erstem Run (nach Schema-Änderung)

Bei Schema-Änderungen (z.B. scd_key-Spalte entfernt) muss die alte
Tabelle manuell in CloudBeaver gedroppt werden:

```sql
DROP TABLE IF EXISTS nessie.curated.snp_fondspreise_scd2;
DROP TABLE IF EXISTS nessie.curated.snp_fondspreise_scd2_test;
```

---

_AP-4b-refactor: Ursprünglich mit `scd_key`-Workaround implementiert
(isin || '|' || business_date). Nach Adapter-Kompatibilitätstest auf
nativen Listen-`unique_key` umgestellt._
