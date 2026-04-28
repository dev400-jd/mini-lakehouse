# Demo 1 Drehbuch — Fondspreis-Zeitreihen in Iceberg

**Ziel:** Tims Frage *"Welcher Fondspreis galt am 20.04.?"* beantworten —
Audit-Spur (Iceberg-Snapshots) und fachliche Wahrheit (SCD2) in derselben
Sandbox sichtbar machen.

**Dauer:** ca. 18 Minuten Live-Demo (+ ca. 5 Minuten Setup vor dem Vortrag).

**Werkzeuge:** Folien (Folie 4-7), CloudBeaver (`http://localhost:8978`),
Terminal (Bash/PowerShell, Repo-Root), VS Code (`dbt/snapshots/...`),
Docker Desktop mit laufendem Stack.

> Dieses Drehbuch ist **keine** Vorlese-Vorlage. Die Sprech-Anker sind
> ganze Saetze, die den Hauptgedanken der Station tragen — Josef
> formuliert sie in eigenen Worten aus.

---

## Vor der Demo

Etwa 5 Minuten vor Demo-Start:

1. **Stack hochfahren und Status pruefen**
   ```bash
   docker compose up -d
   docker compose ps                # alle Services healthy?
   ```
2. **Demo-Startzustand herstellen** (idempotent, ca. 1-2 Minuten)
   ```bash
   ./scripts/reset-demo1.sh
   ```
3. **Smoke-Test in CloudBeaver** (Login mit Demo-Credentials)
   ```sql
   SELECT COUNT(*) FROM nessie.raw.fondspreise;                   -- 1
   SELECT COUNT(*) FROM nessie.staging.stg_fondspreise;           -- 450
   SELECT COUNT(*) FROM nessie.curated.snp_fondspreise_scd2;      -- 450
   ```
4. **Folien & Anwendungen vorbereiten**
   - [ ] Folie 6 (Demo-Choreografie) als Startbild
   - [ ] CloudBeaver-Tab eingeloggt, leerer SQL-Editor offen
   - [ ] Terminal in Repo-Root, Schriftgroesse hochgesetzt
   - [ ] VS Code mit `dbt/snapshots/snp_fondspreise_scd2.sql` offen
   - [ ] Notifications stumm (Slack, Mail, Teams)
   - [ ] Browser-Verlauf/Bookmarks ohne Sensibles

Wenn 1-3 nicht durchlaufen: `./scripts/reset-demo1.sh` einfach erneut
ausfuehren — das Skript ist auf wiederholten Aufruf ausgelegt.

---

## Stationen-Uebersicht

| #  | Station                | Bildschirm           | Dauer    |
|----|------------------------|----------------------|----------|
| 1  | Setup-Erinnerung       | Folie 6              | 1 Min    |
| 2  | Initial-Stand          | CloudBeaver          | 2-3 Min  |
| 3  | Korrektur-Load         | Terminal             | 2 Min    |
| 4  | Snapshots inspizieren  | CloudBeaver          | 3 Min    |
| 5  | Time-Travel-Queries    | CloudBeaver          | 4 Min    |
| 6  | SCD2-Aufloesung        | VS Code -> CloudBeaver | 4-5 Min |
| 7  | Zusammenfassung        | Folie 6/7            | 1-2 Min  |
|    |                        | **Summe**            | **17-20 Min** |

---

## Station 1 — Setup-Erinnerung

- **Dauer:** 1 Minute
- **Bildschirm:** Folie 6 (Demo-Choreografie)
- **Was passiert:** Josef wiederholt das Tim-Szenario aus Folie 4
  und kuendigt an, dass die naechsten 18 Minuten genau dieses
  Szenario in der Sandbox durchgespielt werden.

### Befehl/Query

Keiner — nur Folie zeigen.

### Erwartung

Publikum hat die zwei Loads (Morgen / Nachmittag) und Tims Frage
"Welcher Preis galt am 20.04.?" praesent.

### Sprech-Anker

> "Wir kennen Tims Frage von Folie 4: er hat morgens den Preis
> fuer den 20. April geladen, am Nachmittag kam eine Korrektur
> nach. Beide Loads sollen nachvollziehbar bleiben — aber nur
> einer ist fachlich gueltig. Genau das spielen wir jetzt durch."

### Was-wenn

- *Vortrag laeuft schon hinterher:* Diese Station auf 30 Sekunden
  kuerzen, direkt zu Station 2.

### Uebergang zur naechsten Station

Folie 6 ausblenden, CloudBeaver-Tab in den Vordergrund.
"Schauen wir uns den Stand jetzt an."

---

## Station 2 — Initial-Stand

- **Dauer:** 2-3 Minuten
- **Bildschirm:** CloudBeaver
- **Was passiert:** Den aktuellen Inhalt von `raw.fondspreise`
  anzeigen — eine einzige Iceberg-Zeile, der komplette JSON-File
  als `raw_payload`, dazu die Provenance-Spalten (`source_version`,
  `source_file_hash`, `source_file_size_bytes`).

### Befehl/Query

```sql
SELECT
    ingestion_id,
    ingestion_timestamp,
    source_version,
    source_file_size_bytes,
    source_file_hash
FROM nessie.raw.fondspreise
ORDER BY ingestion_timestamp;
```

Optional, falls Zeit (zeigt File-Inhalt fuer Tims ISIN am 20.04.):

```sql
SELECT
    json_extract_scalar(rec, '$.isin')          AS isin,
    json_extract_scalar(rec, '$.business_date') AS business_date,
    CAST(json_extract_scalar(rec, '$.nav') AS DOUBLE) AS nav
FROM nessie.raw.fondspreise r
CROSS JOIN UNNEST(
    CAST(json_extract(r.raw_payload, '$.records') AS ARRAY(JSON))
) AS u(rec)
WHERE json_extract_scalar(rec, '$.isin') = 'DE000A1JX0V2'
  AND json_extract_scalar(rec, '$.business_date') = '2026-04-20';
```

Optional Source-Tests (`not_null` auf Provenance-Spalten — fuer
Publikum, das fragt "wie pruefen wir dass jeder Row eine Provenance hat?"):

```bash
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select source:raw.fondspreise"
```

### Erwartung

- Erste Query: 1 Zeile, `source_version = 'v1'`,
  `ingestion_timestamp = 2026-04-20 08:15:00 UTC`,
  `source_file_hash` als sha256-Praefix.
- Optionale Query: 1 Datensatz mit `nav = 143.80`.

### Sprech-Anker

> "Im Raw-Layer steht der Load 1 als **eine** Iceberg-Zeile. Im
> Feld `raw_payload` liegt der **komplette** JSON-Inhalt — der Hash
> daneben beweist, dass das byte-identisch zur Quelldatei ist.
> Damit haben wir Audit-Faehigkeit auf Datei-Ebene, nicht nur auf
> Datensatz-Ebene."

### Was-wenn

- *Tabelle leer oder nicht vorhanden:* `reset-demo1.sh` ist nicht
  durchgelaufen. Im Terminal `./scripts/reset-demo1.sh` erneut
  ausfuehren — dauert ca. 1-2 Minuten, dem Publikum kurz erklaeren
  ("kurzer Reset, das ist Teil der Demo-Wiederholbarkeit").
- *CloudBeaver verbindet nicht:* Connection in der Seitenleiste
  rechtsklick -> `Invalidate / Reconnect`.

### Uebergang zur naechsten Station

"Tim's Korrektur kommt jetzt — wir wechseln ins Terminal."

---

## Station 3 — Korrektur-Load

- **Dauer:** 2 Minuten
- **Bildschirm:** Terminal
- **Was passiert:** Den Load-2-Korrektur-File live ingestieren —
  selber Tag, anderer Zeitstempel (14:37 Uhr). Iceberg legt
  automatisch einen zweiten Snapshot an, ohne dass wir das selbst
  triggern muessen.

### Befehl/Query

```bash
docker compose exec spark-master spark-submit /scripts/ingest-fondspreise.py --file /data/sample/fondspreise_load2_correction.json --ingestion-timestamp 2026-04-22T14:37:00Z
```

Direkt danach kurz in CloudBeaver pruefen:

```sql
SELECT COUNT(*) FROM nessie.raw.fondspreise;
```

### Erwartung

- Spark-submit endet mit "Records written: 1 (file-level)" und
  "snapshot count for raw.fondspreise: 2".
- Count-Query liefert `2`.

### Sprech-Anker

> "Jetzt kommt Tims Korrektur als zweiter Load — selber Tag, andere
> Uhrzeit. Achtet auf zwei Dinge: anderer `ingestion_timestamp`,
> und Iceberg legt einen neuen Snapshot an, ohne dass wir das
> selbst aufrufen muessen."

### Was-wenn

- *spark-master nicht erreichbar:* `docker compose ps spark-master`
  pruefen, ggf. `docker compose up -d spark-master`.
- *"no such file"-Fehler:* Git-Bash-Pfadkonvertierung —
  `MSYS_NO_PATHCONV=1` voranstellen oder denselben Befehl in
  PowerShell ausfuehren.

### Uebergang zur naechsten Station

"Iceberg hat jetzt zwei Snapshots. Schauen wir, wie das in der
Metadaten-Tabelle aussieht."

---

## Station 4 — Snapshots inspizieren

- **Dauer:** 3 Minuten
- **Bildschirm:** CloudBeaver
- **Was passiert:** Iceberg-Metadaten-Tabelle abfragen. Beide
  Snapshots werden mit unterschiedlichen `committed_at` und
  `snapshot_id` sichtbar. **Snapshot-IDs werden fuer Station 5
  benoetigt — kopieren!**

### Befehl/Query

```sql
SELECT
    snapshot_id,
    committed_at,
    operation,
    summary['added-records'] AS added_records
FROM nessie.raw."fondspreise$snapshots"
ORDER BY committed_at;
```

### Erwartung

Zwei Zeilen:
- Snapshot 1: `committed_at` ~08:15 UTC, `operation = 'append'`,
  `added_records = 1`
- Snapshot 2: `committed_at` ~14:37 UTC, `operation = 'append'`,
  `added_records = 1`

Snapshot-IDs sind 19-stellige Long-Integer — fuer Station 5 in
einem Notepad oder einer zweiten CloudBeaver-Tab parken.

### Sprech-Anker

> "Iceberg merkt sich, was wann passiert ist — ohne dass wir das
> selbst bauen mussten. Das ist die Audit-Spur."

### Was-wenn

- *Nur 1 Snapshot sichtbar:* Station 3 ist nicht durchgelaufen.
  Spark-submit-Output im Terminal kontrollieren, ggf. wiederholen.
- *committed_at zeigt eine andere Stunde:* Iceberg speichert UTC,
  CloudBeaver kann lokale Zeitzone anzeigen — kurz erwaehnen,
  nicht irritieren lassen. *Konzept Snapshot vs. Stichtag ist auf
  Folie 5 erklaert.*

### Uebergang zur naechsten Station

"Beide Snapshot-IDs habe ich. Jetzt die spannende Frage: kann ich
in beide Zustaende **zurueckblicken**?"

---

## Station 5 — Time-Travel-Queries

- **Dauer:** 4 Minuten
- **Bildschirm:** CloudBeaver
- **Was passiert:** Dieselbe `SELECT`-Anweisung, zweimal — gegen
  zwei verschiedene Snapshot-IDs mit `FOR VERSION AS OF`. Snapshot 1
  zeigt nur den Morgen-Load, Snapshot 2 zeigt beide.

### Befehl/Query

Snapshot-IDs aus Station 4 einsetzen (`<SNAP1>`, `<SNAP2>`):

```sql
-- Stand nach Load 1
SELECT COUNT(*) AS rows
FROM nessie.raw.fondspreise FOR VERSION AS OF <SNAP1>;
```

```sql
-- Stand nach Load 2
SELECT COUNT(*) AS rows
FROM nessie.raw.fondspreise FOR VERSION AS OF <SNAP2>;
```

Optional, falls Zeit — Time Travel auf Staging zeigt, dass das
Pattern auf jeder Iceberg-Tabelle funktioniert:

```sql
SELECT COUNT(*) FROM nessie.staging.stg_fondspreise
FOR VERSION AS OF <STG_SNAP_ID>;
```

### Erwartung

- Erste Query: `1` Row (nur Morgen-Load sichtbar).
- Zweite Query: `2` Rows (beide Loads sichtbar).

### Sprech-Anker

> "Dasselbe SQL, zweimal — aber gegen unterschiedliche Snapshots.
> Das beantwortet 'Was haben wir wann geladen?' — aber noch nicht
> 'Welcher Preis galt fachlich?'"

### Was-wenn

- *"Snapshot not found"*: ID falsch kopiert (z.B. fuehrendes Komma
  oder eine Stelle abgeschnitten). Aus Station 4 neu kopieren.
- *Beide Counts identisch:* vermutlich zweimal dieselbe ID
  eingesetzt. Station 4 wiederholen, IDs sorgfaeltig nehmen.

### Uebergang zur naechsten Station

"Audit-Spur ist klar. Aber Tims eigentliche Frage war fachlich:
welcher Preis **gilt** jetzt? Dafuer brauchen wir die
SCD2-Aufloesung."

---

## Station 6 — SCD2-Aufloesung

- **Dauer:** 4-5 Minuten
- **Bildschirm:** VS Code -> CloudBeaver
- **Was passiert:** Erst kurz in VS Code den dbt-Snapshot-Code
  zeigen (Snapshot-Block, `unique_key`, `strategy='check'`,
  `check_cols`). Dann zurueck zu CloudBeaver: das Resultat in
  `curated.snp_fondspreise_scd2` zeigt **zwei** Zeilen fuer
  (Tim's ISIN, 20.04.) — eine historische, eine aktuelle.

### Befehl/Query

In VS Code: `dbt/snapshots/snp_fondspreise_scd2.sql` oeffnen
(`Ctrl+P` -> Datei tippen). Auf den `snapshot`-Block zeigen,
besonders `unique_key=['isin','business_date']` und
`check_cols=['nav','currency']`.

**Wichtig:** Nach Load 2 muss zuerst Staging refreshed werden,
**dann** der Snapshot — sonst arbeitet `dbt snapshot` mit
veralteten Staging-Daten und sieht die Korrektur nicht.

```bash
# 1. Staging neu bauen — zieht jetzt beide Loads
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt run --select stg_fondspreise"
```

```bash
# 2. Snapshot laufen lassen — vergleicht Staging mit alter Snapshot-Version
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt snapshot --select snp_fondspreise_scd2"
```

Optional, dbt-Tests gegen den Snapshot (zeigt: Eindeutigkeit von
`dbt_scd_id`, `not_null` auf Schluesseln, `unique_combination_of_columns`
auf `(isin, business_date)` fuer aktuell-gueltige Rows):

```bash
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select snp_fondspreise_scd2"
```

In CloudBeaver:

```sql
SELECT
    isin,
    business_date,
    nav,
    source_version,
    dbt_valid_from,
    dbt_valid_to
FROM nessie.curated.snp_fondspreise_scd2
WHERE isin = 'DE000A1JX0V2'
  AND business_date = DATE '2026-04-20'
ORDER BY dbt_valid_from;
```

### Erwartung

Zwei Zeilen:
- Zeile 1: `nav = 143.80`, `source_version = 'v1'`,
  `dbt_valid_to` **gesetzt** (= historisch)
- Zeile 2: `nav = 143.12`, `source_version = 'v1-correction'`,
  `dbt_valid_to = NULL` (= aktuell gueltig)

### Sprech-Anker

> "Das ist die fachliche Antwort: 143,12 Euro ist der gueltige Preis."

### Was-wenn

- *Nur eine Zeile sichtbar:* Staging wurde noch nicht refreshed.
  Beide Befehle oben in der Reihenfolge `dbt run` -> `dbt snapshot`
  ausfuehren.
- *Beide Zeilen mit `dbt_valid_to = NULL`:* `dbt snapshot` lief vor
  dem Staging-Refresh — selbe Reihenfolge nochmal ausfuehren, dann
  Query wiederholen.
- *Snapshot meldet 0 Aenderungen:* heisst `stg_fondspreise` zeigt
  noch den alten Stand. Erst `dbt run --select stg_fondspreise`,
  dann erneut `dbt snapshot`.
- *VS Code findet die Datei nicht:* `Ctrl+P` ->
  `snp_fondspreise_scd2`.

### Uebergang zur naechsten Station

"Tim hat seine Antwort. Lasst uns das kurz einordnen, bevor wir zu
Demo 2 wechseln."

---

## Station 7 — Zusammenfassung

- **Dauer:** 1-2 Minuten
- **Bildschirm:** Folie 6 oder 7 (was nach der Demo kommt)
- **Was passiert:** Rueckbezug zu Tims Frage. Kernaussage: Audit-Spur
  und fachliche Wahrheit sind **zwei** Schichten — beide notwendig,
  fuer unterschiedliche Adressaten.

### Befehl/Query

Keiner — nur Folie zeigen.

### Erwartung

Publikum kann den Unterschied "Iceberg-Snapshots = wann kam was
rein" vs "SCD2 = was gilt fachlich" in eigenen Worten formulieren.

### Sprech-Anker

> "Zwei Schichten Wahrheit nebeneinander: die Audit-Spur in Iceberg
> beantwortet 'Was haben wir wann geladen?'. Der SCD2-Snapshot
> beantwortet 'Was ist fachlich aktuell?'. Beide brauchen wir —
> einer ist Compliance-relevant, der andere Reporting-relevant."

### Was-wenn

- *Frage aus dem Publikum, die Demo 2 vorgreifen wuerde:*
  parken — "Gute Frage, da kommen wir gleich in Demo 2 drauf."
- *Zeit ueberzogen:* Diese Station auf 30 Sekunden kuerzen, direkt
  zur naechsten Folie.

### Uebergang zur naechsten Station

Wechsel zur Demo-2-Einleitungsfolie. Optional Setup fuer Demo 2
schon im Hintergrund anstossen (siehe "Nach der Demo").

---

## Nach der Demo

**Erfolgreicher Lauf:**
- Folien-Tab in den Vordergrund, weiter mit Demo-2-Einleitung.
- Optional: `./scripts/demo2-state.sh raw_cur` im Hintergrund
  starten — der State-Aufbau dauert ca. 100 Sekunden und faellt
  in die Folien-Phase 9-11.

**Demo musste mittendrin abgebrochen werden:**
- `./scripts/reset-demo1.sh` zurueck zum Startzustand. Das Skript
  ist idempotent — kein manuelles Aufraeumen noetig.

**Wiederholung in derselben Session (z.B. zweite Gruppe):**
- `./scripts/reset-demo1.sh` reicht, dauert 1-2 Minuten.

**Demo abschliessen / Ressourcen freigeben:**
- Stack laufen lassen ist ok (idempotent ueber Sessions hinweg).
- Falls voller Reset gewuenscht:
  ```bash
  docker compose down -v       # !! WARNUNG: loescht MinIO/Postgres-Daten
  docker compose up -d
  ```

---

## Anhang: Befehlsreferenz (Copy-Paste)

Kompakte Sammlung aller Befehle aus dem Drehbuch — fuer schnelles
Nachschlagen ohne durch die Stationen zu scrollen.

### Setup vor der Demo

```bash
docker compose up -d
docker compose ps
./scripts/reset-demo1.sh
```

### Smoke-Test

```sql
SELECT COUNT(*) FROM nessie.raw.fondspreise;                   -- 1
SELECT COUNT(*) FROM nessie.staging.stg_fondspreise;           -- 450
SELECT COUNT(*) FROM nessie.curated.snp_fondspreise_scd2;      -- 450
```

### Station 2 — Raw-Stand

```sql
SELECT
    ingestion_id, ingestion_timestamp, source_version,
    source_file_size_bytes, source_file_hash
FROM nessie.raw.fondspreise
ORDER BY ingestion_timestamp;
```

```sql
SELECT
    json_extract_scalar(rec, '$.isin')          AS isin,
    json_extract_scalar(rec, '$.business_date') AS business_date,
    CAST(json_extract_scalar(rec, '$.nav') AS DOUBLE) AS nav
FROM nessie.raw.fondspreise r
CROSS JOIN UNNEST(
    CAST(json_extract(r.raw_payload, '$.records') AS ARRAY(JSON))
) AS u(rec)
WHERE json_extract_scalar(rec, '$.isin') = 'DE000A1JX0V2'
  AND json_extract_scalar(rec, '$.business_date') = '2026-04-20';
```

### Station 3 — Korrektur-Load

```bash
docker compose exec spark-master spark-submit /scripts/ingest-fondspreise.py --file /data/sample/fondspreise_load2_correction.json --ingestion-timestamp 2026-04-20T14:37:00Z
```

```sql
SELECT COUNT(*) FROM nessie.raw.fondspreise;                   -- 2
```

### Station 4 — Snapshots

```sql
SELECT
    snapshot_id, committed_at, operation,
    summary['added-records'] AS added_records
FROM nessie.raw."fondspreise$snapshots"
ORDER BY committed_at;
```

### Station 5 — Time Travel

```sql
SELECT COUNT(*) FROM nessie.raw.fondspreise FOR VERSION AS OF <SNAP1>;
SELECT COUNT(*) FROM nessie.raw.fondspreise FOR VERSION AS OF <SNAP2>;
```

### Station 6 — SCD2

```bash
# 1. Staging refreshen — Pflicht nach Load 2
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt run --select stg_fondspreise"

# 2. Erst dann Snapshot
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt snapshot --select snp_fondspreise_scd2"

# 3. Optional: Tests gegen Snapshot
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select snp_fondspreise_scd2"
```

Optional Source-Tests zu Station 2:

```bash
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select source:raw.fondspreise"
```

```sql
SELECT
    isin, business_date, nav, source_version,
    dbt_valid_from, dbt_valid_to
FROM nessie.curated.snp_fondspreise_scd2
WHERE isin = 'DE000A1JX0V2'
  AND business_date = DATE '2026-04-20'
ORDER BY dbt_valid_from;
```

### Konkretwerte fuer den Faktencheck

- **Tim's Fonds (ISIN):** `DE000A1JX0V2`
- **Datum:** `2026-04-20` (Montag)
- **Vortag:** `2026-04-17` (Freitag, regulaerer Werte-Verlauf)
- **Folgetag:** `2026-04-21` (Dienstag, regulaerer Werte-Verlauf)
- **Load 1 (`v1`):** `nav = 143.80 EUR`, ingestion 08:15 UTC
- **Load 2 (`v1-correction`):** `nav = 143.12 EUR`, ingestion 14:37 UTC
- **Records pro Load:** 5 Fonds x 90 Handelstage = 450
- **Snapshots in `raw.fondspreise` nach Demo:** 2
- **SCD2-Versionen fuer (`DE000A1JX0V2`, `2026-04-20`):** 2

> **Hinweis zu den Werten:** Die NAV-Zahlen `143,80` / `143,12`
> entsprechen dem geplanten Folien-Stand. Falls der Generator
> (`scripts/generate-fondspreise.py`, Seed `20260422`) andere Werte
> liefert: vor dem Workshop den Generator anpassen oder die
> Folienwerte angleichen — Konsistenz ist wichtiger als der
> konkrete Wert.

### Troubleshooting-Schnellreferenz

| Symptom                                    | Schneller Fix                                  |
|--------------------------------------------|------------------------------------------------|
| `raw.fondspreise` leer                     | `./scripts/reset-demo1.sh`                     |
| Snapshot-Tabelle zeigt nur 1 Snapshot      | Station 3 (`spark-submit`) wiederholen         |
| Time-Travel-Query: "snapshot not found"    | Snapshot-ID neu aus Station 4 kopieren         |
| spark-submit faellt mit Path-Fehler        | `MSYS_NO_PATHCONV=1` voranstellen / PowerShell |
| CloudBeaver verbindet nicht                | Connection rechtsklick -> Invalidate           |
| `dbt snapshot` zeigt 0 Aenderungen         | Staging nicht refreshed — erst `dbt run --select stg_fondspreise`, dann erneut `dbt snapshot` |
| SCD2-Query zeigt nur 1 Zeile               | Reihenfolge aus Station 6: erst `dbt run stg_fondspreise`, dann `dbt snapshot` |
