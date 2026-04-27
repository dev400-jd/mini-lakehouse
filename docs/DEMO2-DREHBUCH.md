# Demo 2 Drehbuch — ESG-Layer-Wanderung mit Quality Gate

**Ziel:** Layer-Durchstich am ESG-Beispiel (Raw -> Staging -> Curated -> Trusted)
mit Quality-Gate-Hoehepunkt — und sichtbarer Multi-Source-Realitaet
(NZDPU + CDP) inklusive Datenqualitaets-Issues.

**Dauer:** ca. 25 Minuten Live-Demo (+ ca. 5 Minuten Setup vor dem Vortrag).

**Werkzeuge:** Folien (Folie 10-12), CloudBeaver (`http://localhost:8978`),
Terminal (Bash/PowerShell, Repo-Root), VS Code (`dbt/models/`), optional
Browser fuer Data Docs HTML, Docker Desktop mit laufendem Stack.

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
2. **Demo-Startzustand setzen** (idempotent, ca. 100 Sekunden)
   ```bash
   ./scripts/demo2-state.sh raw_cur
   ```
   Zielzustand: Raw + Staging + Curated befuellt, **Trusted leer**.
3. **Smoke-Test in CloudBeaver**
   ```sql
   SELECT COUNT(*) FROM nessie.raw.cdp_emissions;                 -- 1
   SELECT COUNT(*) FROM nessie.raw.nzdpu_emissions;               -- 1
   SELECT COUNT(*) FROM nessie.staging.stg_cdp_emissions;         -- 100
   SELECT COUNT(*) FROM nessie.staging.stg_nzdpu_emissions;       -- 90
   SELECT COUNT(*) FROM nessie.curated.curated_companies;         -- 30
   SELECT COUNT(*) FROM nessie.curated.curated_esg_emissions;     -- 150
   ```
   Eine Trusted-Pruefung ist bewusst nicht dabei — die Tabelle soll
   noch nicht existieren (Station 7 baut sie live).
4. **Folien & Anwendungen vorbereiten**
   - [ ] Folie 10 (Demo-2-Bruecke) als Startbild
   - [ ] CloudBeaver-Tab eingeloggt, leerer SQL-Editor offen
   - [ ] Terminal in Repo-Root, Schriftgroesse hochgesetzt
   - [ ] PowerShell: `$env:PYTHONIOENCODING = "utf-8"` einmalig setzen
         (oder Bash mit `export PYTHONIOENCODING=utf-8` aus `~/.bashrc`)
   - [ ] VS Code mit `dbt/models/staging/stg_nzdpu_emissions.sql` und
         `dbt/models/curated/curated_esg_emissions.sql` als Tabs offen
   - [ ] Optional: Browser-Tab mit Pfad zu
         `great_expectations/uncommitted/data_docs/local_site/index.html`
   - [ ] Notifications stumm

Wenn 1-3 nicht durchlaufen: `./scripts/demo2-state.sh raw_cur` einfach
erneut ausfuehren — die State-Machine ist auf wiederholten Aufruf ausgelegt.

---

## Stationen-Uebersicht

| #  | Station                              | Bildschirm                       | Dauer    |
|----|--------------------------------------|----------------------------------|----------|
| 1  | Setup-Erinnerung                     | Folie 10                         | 1 Min    |
| 2  | Raw — Zwei Quellen, zwei Formate     | CloudBeaver                      | 3 Min    |
| 3  | Staging NZDPU — Doppelter UNNEST     | VS Code -> CloudBeaver           | 3 Min    |
| 4  | Staging CDP — CSV mit Datenqualitaet | CloudBeaver                      | 3 Min    |
| 5  | Curated — Harmonisierung             | VS Code -> CloudBeaver           | 4 Min    |
| 6  | Cross-Source-Konflikt                | CloudBeaver                      | 2-3 Min  |
| 7  | Quality Gate — Gruener Lauf          | Terminal -> CloudBeaver          | 3 Min    |
| 8  | Quality Gate — Roter Lauf            | CloudBeaver -> Terminal -> CB    | 4 Min    |
| 9  | Zusammenfassung                      | Folie 12                         | 1-2 Min  |
|    |                                      | **Summe**                        | **24-26 Min** |

---

## Station 1 — Setup-Erinnerung

- **Dauer:** 1 Minute
- **Bildschirm:** Folie 10
- **Was passiert:** Bruecke von Demo 1 (saubere Fondspreise) zu Demo 2
  (ESG-Realitaet mit zwei Quellen und Datenqualitaets-Luecken).

### Befehl/Query

Keiner — nur Folie zeigen.

### Erwartung

Publikum hat die zwei Quellen (NZDPU als JSON, CDP als CSV) und das
Stichwort "Multi-Source-Realitaet" praesent.

### Sprech-Anker

> "Demo 1 war die ideale Welt: eine Quelle, sauberes Format, klare
> Korrektur. Demo 2 ist die echte Welt: zwei Quellen, zwei Formate,
> Luecken in den Daten. Wir wandern jetzt durch alle vier Layer und
> sehen am Ende, warum wir ein externes Quality Gate brauchen."

### Was-wenn

- *Vortrag laeuft schon hinterher:* Diese Station auf 30 Sekunden
  kuerzen, direkt zu Station 2.

### Uebergang zur naechsten Station

Folie ausblenden, CloudBeaver in den Vordergrund.
"Schauen wir zuerst, was im Raw-Layer steht."

---

## Station 2 — Raw: Zwei Quellen, zwei Formate

- **Dauer:** 3 Minuten
- **Bildschirm:** CloudBeaver
- **Was passiert:** Beide Raw-Tabellen nebeneinander zeigen. File-level
  Provenance fuer beide Quellen identisch, aber `raw_payload`-Struktur
  unterschiedlich (CSV-Text vs. JSON-Wrapper). `source_file_format`
  macht das explizit.

### Befehl/Query

```sql
SELECT
    source_system,
    source_version,
    source_file_format,
    source_file_size_bytes,
    source_file_hash
FROM nessie.raw.cdp_emissions
UNION ALL
SELECT
    source_system,
    source_version,
    source_file_format,
    source_file_size_bytes,
    source_file_hash
FROM nessie.raw.nzdpu_emissions;
```

Optional, falls Zeit (zeigt die ersten Zeichen beider Payloads):

```sql
SELECT 'cdp'   AS src, substring(raw_payload, 1, 120) AS preview FROM nessie.raw.cdp_emissions
UNION ALL
SELECT 'nzdpu' AS src, substring(raw_payload, 1, 120) AS preview FROM nessie.raw.nzdpu_emissions;
```

### Erwartung

- Erste Query: 2 Zeilen, `source_file_format` `csv` und `json`,
  unterschiedliche Hashes und Sizes.
- Optionale Query: CDP-Preview zeigt CSV-Header, NZDPU-Preview zeigt
  `{"status":"ok",...`.

### Sprech-Anker

> "Die Realitaet: Daten kommen aus verschiedenen Quellen, mit
> verschiedenen Formaten — Raw bewahrt sie alle. Eine Iceberg-Zeile
> pro Datei, byte-identisch zum Original."

### Was-wenn

- *Eine der beiden Tabellen leer:* `./scripts/demo2-state.sh raw_cur`
  erneut ausfuehren. Dauer ca. 100 Sekunden — Publikum kurz Bescheid
  geben.
- *Wenn jemand fragt "wie sieht das auf Raw-Ebene allein aus":*
  `./scripts/demo2-state.sh raw` zeigt nur den Raw-Zustand.

### Uebergang zur naechsten Station

"Jetzt schauen wir, wie Staging die JSON-Schachtelung von NZDPU aufloest."

---

## Station 3 — Staging NZDPU: Doppelter UNNEST

- **Dauer:** 3 Minuten
- **Bildschirm:** VS Code -> CloudBeaver
- **Was passiert:** In VS Code `stg_nzdpu_emissions.sql` zeigen — zwei
  `CROSS JOIN UNNEST` ueber `data[]` und `reporting_periods[]`. In
  CloudBeaver Ergebnis: 90 Rows aus 30 Companies x 3 Jahren.

### Befehl/Query

In VS Code: `dbt/models/staging/stg_nzdpu_emissions.sql` oeffnen
(`Ctrl+P`). Auf die zwei `CROSS JOIN UNNEST`-Bloecke zeigen.

In CloudBeaver:

```sql
SELECT COUNT(*)              AS total_rows,
       COUNT(DISTINCT isin)  AS distinct_isins
FROM nessie.staging.stg_nzdpu_emissions;
```

Stichprobe Siemens AG:

```sql
SELECT isin, reporting_year, scope_1_tco2e, scope_3_total_tco2e
FROM nessie.staging.stg_nzdpu_emissions
WHERE isin = 'DE0007236101'
ORDER BY reporting_year;
```

### Erwartung

- Count-Query: `total_rows = 90`, `distinct_isins = 30`.
- Siemens-Query: 3 Zeilen (2021, 2022, 2023). `scope_1_tco2e`
  zwischen 3,0 und 3,8 Mio. `scope_3_total_tco2e` ist 2021/2022 NULL
  und 2023 gefuellt — die NZDPU-Quelle liefert Scope 3 nur fuer das
  letzte Jahr.

### Sprech-Anker

> "JSON-Schachtelung wird in Staging aufgeloest — eine Zeile pro
> Company-Jahr-Kombination. 30 Unternehmen mal 3 Jahre, also 90 Rows."

### Was-wenn

- *VS Code findet Datei nicht:* `Ctrl+P` -> `stg_nzdpu_emissions`.
- *Count zeigt nicht 90:* State-Machine pruefen — ggf.
  `./scripts/demo2-state.sh raw_stg` und neu starten.

### Uebergang zur naechsten Station

"NZDPU sieht nach JSON-Magie sauber aus. Schauen wir, wie CDP aussieht
— da kommt die Realitaet einer CSV-Quelle."

---

## Station 4 — Staging CDP: CSV mit Datenqualitaet

- **Dauer:** 3 Minuten
- **Bildschirm:** CloudBeaver
- **Was passiert:** `stg_cdp_emissions` zeigen. 100 Rows total, davon
  40 mit NULL-ISIN und 11 mit NULL-scope_1. Das ist die Realitaet von
  CSV-Quellen.

### Befehl/Query

```sql
SELECT
    COUNT(*)                                          AS rows_total,
    COUNT(*) FILTER (WHERE isin IS NULL)              AS null_isin,
    COUNT(*) FILTER (WHERE scope_1_tco2e IS NULL)     AS null_scope_1,
    COUNT(DISTINCT emission_unit)                     AS distinct_units
FROM nessie.staging.stg_cdp_emissions;
```

Optional, falls Zeit (zeigt die Inkonsistenz im `emission_unit`):

```sql
SELECT emission_unit, COUNT(*) AS n
FROM nessie.staging.stg_cdp_emissions
GROUP BY emission_unit
ORDER BY n DESC;
```

### Erwartung

- `rows_total = 100`, `null_isin = 40`, `null_scope_1 = 11`,
  `distinct_units = 3` (zwei Unit-Schreibweisen plus NULL).
- Optionale Query: `metric tons CO2e`, `tonnes CO2`, NULL — drei
  Auspraegungen fuer denselben Sachverhalt.

### Sprech-Anker

> "Staging zeigt die Wahrheit der Daten — inklusive Luecken. 40 Prozent
> der CDP-Records haben keine ISIN, 11 Prozent kein Scope 1, und die
> Einheit wird mal mit, mal ohne Schreibvariante geliefert. Diese
> Records muessen wir spaeter im Curated bewusst behandeln."

### Was-wenn

- *NULL-Counts weichen ab:* das Sample-File wurde geaendert. Drehbuch-
  Anhang nennt die Erwartungswerte.

### Uebergang zur naechsten Station

"Jetzt sehen wir, was Curated daraus macht — Harmonisierung mit Macros
und ISIN-Filter."

---

## Station 5 — Curated: Harmonisierung

- **Dauer:** 4 Minuten
- **Bildschirm:** VS Code -> CloudBeaver
- **Was passiert:** In VS Code `curated_esg_emissions.sql` zeigen —
  `UNION ALL` der beiden Staging-Quellen, `WHERE isin IS NOT NULL` als
  Filter, Macros `standardize_isin` und `to_decimal`. In CloudBeaver:
  150 Rows in `curated_esg_emissions` (90 nzdpu + 60 cdp ohne NULL-ISIN),
  30 Rows in `curated_companies`.

### Befehl/Query

In VS Code: `dbt/models/curated/curated_esg_emissions.sql` und
`dbt/models/curated/curated_companies.sql` zeigen — auf den
`{{ standardize_isin(...) }}`-Macro-Aufruf und das
`WHERE isin IS NOT NULL` zeigen.

In CloudBeaver:

```sql
SELECT source_system, COUNT(*) AS rows
FROM nessie.curated.curated_esg_emissions
GROUP BY source_system
ORDER BY source_system;
```

```sql
SELECT COUNT(*) AS companies FROM nessie.curated.curated_companies;
```

### Erwartung

- Erste Query: `cdp = 60`, `nzdpu = 90`. Summe 150.
- Zweite Query: `companies = 30`.

### Sprech-Anker

> "Curated harmonisiert — aber bewahrt noch beide Quellen, weil wir
> noch keine Source-of-Truth-Entscheidung getroffen haben. NULL-ISINs
> sind raus, ISIN-Format vereinheitlicht, beide Quellen koexistieren
> mit `source_system` als drittem Schluessel."

### Was-wenn

- *60 vs. 90 zeigen andere Zahlen:* Staging neu pruefen, ggf.
  `./scripts/demo2-state.sh raw_cur` aufrufen.
- *VS Code zeigt unerwartete SQL-Stelle:* Modell wurde geaendert.
  Kommentar im File macht den Refactoring-Stand klar.

### Uebergang zur naechsten Station

"Beide Quellen koexistieren — und liefern fuer dieselbe ISIN
unterschiedliche Werte. Das ist der Kern des Multi-Source-Problems."

---

## Station 6 — Cross-Source-Konflikt

- **Dauer:** 2-3 Minuten
- **Bildschirm:** CloudBeaver
- **Was passiert:** Fuer Siemens AG (DE0007236101) im Berichtsjahr
  2022 die zwei Source-Records nebeneinander zeigen. NZDPU und CDP
  liefern unterschiedliche Scope-1-Werte fuer dasselbe Unternehmen,
  dasselbe Jahr.

### Befehl/Query

```sql
SELECT
    isin,
    reporting_year,
    source_system,
    scope_1_tco2e
FROM nessie.curated.curated_esg_emissions
WHERE isin = 'DE0007236101'
  AND reporting_year = 2022
ORDER BY source_system;
```

Optional fuer das Gesamtbild aller Jahre:

```sql
SELECT
    reporting_year,
    source_system,
    scope_1_tco2e
FROM nessie.curated.curated_esg_emissions
WHERE isin = 'DE0007236101'
ORDER BY reporting_year, source_system;
```

### Erwartung

- Erste Query: 2 Zeilen
  - `cdp`:   `scope_1_tco2e = 3253366.000`
  - `nzdpu`: `scope_1_tco2e = 3794435.000`
- Differenz von ca. 540 000 tCO2e — etwa 17 Prozent.

### Sprech-Anker

> "Echte Asset-Manager-Realitaet: zwei Quellen liefern
> unterschiedliche Werte fuer dasselbe Unternehmen. Welche stimmt?
> Diese Frage beantwortet Curated bewusst nicht — die Source-of-Truth-
> Entscheidung gehoert in den Trusted Layer oder ins Reporting."

### Was-wenn

- *Nur eine Zeile:* Die ISIN existiert nur in einer Quelle (sollte
  hier nicht passieren — alle 30 NZDPU-ISINs sind auch in CDP). Im
  Sandbox `./scripts/demo2-state.sh raw_cur` neu setzen.
- *Frage zur Aufloesung:* "Im Sandbox bevorzugen wir NZDPU fuer
  Stammdaten — siehe `curated_companies` mit `preferred_source`. Die
  Emissions-Quelle waehlen wir bewusst nicht aus, das ist Reporting-
  Aufgabe."

### Uebergang zur naechsten Station

"Bevor diese Daten in Trusted gehen koennen, kommt das Quality Gate.
Schauen wir uns den Normal-Pfad an."

---

## Station 7 — Quality Gate: Gruener Lauf

- **Dauer:** 3 Minuten
- **Bildschirm:** Terminal -> CloudBeaver
- **Was passiert:** `scripts/promote-trusted-esg.py` live ausfuehren.
  Drei Phasen sichtbar: Curated-Refresh, GE-Checkpoint (alle 6
  Expectations gruen), Trusted-Promotion. Trusted wird gebaut.

### Befehl/Query

PowerShell:

```powershell
$env:PYTHONIOENCODING = "utf-8"
uv run python scripts/promote-trusted-esg.py
```

Bash:

```bash
PYTHONIOENCODING=utf-8 uv run python scripts/promote-trusted-esg.py
```

Direkt danach in CloudBeaver:

```sql
SELECT COUNT(*) AS rows FROM nessie.trusted.trusted_esg_emissions;
```

### Erwartung

Terminal-Output (Auszuege):
- `PHASE 1: Curated-Refresh via dbt` -> Curated 150 Rows
- `PHASE 2: Quality Gate -- Great Expectations`
  - `Expectations evaluiert: 6`
  - `Erfolgreich:            6`
  - `Fehlgeschlagen:         0`
  - `GATE GRUEN -- Promotion freigegeben.`
- `PHASE 3: Trusted-Promotion via dbt` -> Trusted 150 Rows
- `PROMOTION ERFOLGREICH.`
- Exit-Code: `0`

CloudBeaver-Query: `rows = 150` (Trusted ist Pass-through aus Curated).

### Sprech-Anker

> "Drei Phasen: erst Curated bauen, dann Quality Gate pruefen, dann
> bei gruenem Gate nach Trusted promoten. Bei sauberen Daten faellt
> das nicht weiter auf — genau das ist der Normalfall."

### Was-wenn

- *`UnicodeEncodeError` im PowerShell-Aufruf:* `$env:PYTHONIOENCODING = "utf-8"`
  vor dem `uv run` setzen. Bash-Inline-Form (`PYTHONIOENCODING=...`)
  funktioniert in PowerShell **nicht**.
- *`docker compose exec: not running`:* Stack hochfahren mit
  `docker compose up -d`.
- *Phase 2 zeigt unerwartet rote Expectation:* Manipulations-Demo aus
  Station 8 wurde ggf. nicht aufgeraeumt — Cleanup-Statement aus
  Anhang ausfuehren, dann Skript erneut.

### Uebergang zur naechsten Station

"Soweit der Normalfall. Jetzt machen wir die Daten kaputt und sehen,
was passiert."

---

## Station 8 — Quality Gate: Roter Lauf mit Manipulation

- **Dauer:** 4 Minuten
- **Bildschirm:** CloudBeaver -> Terminal -> CloudBeaver -> (optional Browser)
- **Was passiert:** In CloudBeaver einen INSERT in
  `curated_esg_emissions` mit unbekanntem `source_system = 'manipulation'` —
  das verletzt `expect_column_values_to_be_in_set`. Im Terminal das
  Promotion-Skript mit `--skip-curated-refresh` (sonst wuerde Phase 1
  die Manipulation ueberschreiben). GE wird rot, Phase 3 nicht ausgefuehrt,
  Trusted bleibt unveraendert. Optional Browser: Data Docs HTML zeigt den
  Befund. Aufraeumen sofort danach.

### Befehl/Query

In CloudBeaver — Manipulation einfuegen:

```sql
INSERT INTO nessie.curated.curated_esg_emissions
(isin, reporting_year, source_system,
 scope_1_tco2e, scope_2_location_tco2e, scope_2_market_tco2e, scope_3_total_tco2e,
 verification, ingestion_id, ingestion_timestamp, source_file_hash)
VALUES (
 'DE0007236101', 2099, 'manipulation',
 DECIMAL '5000000000.000', NULL, NULL, NULL,
 'manual_demo_violation',
 'demo-violation-uuid',
 TIMESTAMP '2026-04-27 12:00:00',
 'sha256:demo'
);
```

Im Terminal:

```powershell
$env:PYTHONIOENCODING = "utf-8"
uv run python scripts/promote-trusted-esg.py --skip-curated-refresh
```

(Bash: `PYTHONIOENCODING=utf-8 uv run python scripts/promote-trusted-esg.py --skip-curated-refresh`)

Direkt danach in CloudBeaver pruefen, dass Trusted unveraendert ist:

```sql
SELECT COUNT(*) AS rows FROM nessie.trusted.trusted_esg_emissions;
```

Optional im Browser oeffnen:

```
file:///C:/Users/dev400/mini-lakehouse/great_expectations/uncommitted/data_docs/local_site/index.html
```

**Cleanup direkt nach der Demo (Pflicht!):**

```sql
DELETE FROM nessie.curated.curated_esg_emissions
WHERE source_system = 'manipulation';
```

### Erwartung

Terminal-Output:
- `(Phase 1 uebersprungen via --skip-curated-refresh)`
- `PHASE 2: Quality Gate -- Great Expectations`
  - `Erfolgreich:            5`
  - `Fehlgeschlagen:         1`
  - `GATE ROT -- Promotion blockiert.`
  - Verweis auf Data-Docs-Pfad
- Phase 3 wird **nicht** ausgefuehrt
- Exit-Code: `1`

CloudBeaver-Query: `rows = 150` (Trusted unveraendert vom gruenen Lauf
in Station 7 — die Manipulation hat es nicht durchgeschafft).

Optional Browser: Data Docs zeigt eine FAIL-Zeile bei
`expect_column_values_to_be_in_set` mit `unexpected_value: manipulation`.

### Sprech-Anker

> "Quality Gate funktioniert wie ein Tuersteher — problematische Daten
> kommen nicht in Trusted. Curated darf den Befund zeigen, Trusted
> bleibt sauber, und die Promotion-Kette steht still bis das Problem
> behoben ist."

### Was-wenn

- *INSERT scheitert mit Schema-Fehler:* Spalten-Reihenfolge stimmt
  nicht — kompletter INSERT inkl. Spalten-Liste verwenden (oben).
- *Skript laeuft trotzdem in Phase 3:* `--skip-curated-refresh`-Flag
  vergessen? Wenn Phase 1 lief, wurde der Manipulations-Record
  ueberschrieben — INSERT erneut.
- *Data Docs HTML existiert nicht:* in einem fruehen Lauf wurde
  `build_data_docs()` noch nicht aufgerufen. Skript triggert es ueber
  `update_data_docs`-Action automatisch — ein zweiter Lauf reicht.
- *Cleanup vergessen und Demo wiederholen:* `./scripts/demo2-state.sh raw_cur`
  setzt alles zurueck (dauert ca. 100 Sekunden).

### Uebergang zur naechsten Station

"Cleanup ist drin, Curated ist wieder sauber. Lasst uns das kurz
einordnen, bevor wir weitergehen."

---

## Station 9 — Zusammenfassung

- **Dauer:** 1-2 Minuten
- **Bildschirm:** Folie 12
- **Was passiert:** Rueckbezug auf die Layer-Wanderung und die drei
  Rollen von Qualitaet — dbt-Tests im Build, GE als externes Gate,
  Provenance fuer Audit.

### Befehl/Query

Keiner — nur Folie zeigen.

### Erwartung

Publikum kann die drei Rollen "Build-Test", "Gate-Block", "Provenance-
Audit" voneinander abgrenzen — Vorbereitung fuer die folgende
Theorie-Folie.

### Sprech-Anker

> "Drei Rollen von Qualitaet, ein Architekturprinzip: dbt-Tests
> sichern den Build, das externe Gate blockiert die Promotion,
> Provenance erlaubt den Audit. Keine der drei ersetzt die anderen."

### Was-wenn

- *Frage aus dem Publikum:* "Warum nicht alles in dbt?" — kurz
  antworten: dbt-Tests laufen mit dem Build, das Gate ist davon
  unabhaengig und wird **vor** der Promotion gepruefte. Das ist die
  saubere Trennung.

### Uebergang zur naechsten Station

Wechsel zur Folie 13 (oder was nach Demo 2 kommt).

---

## Nach der Demo

**Erfolgreicher Lauf:**
- Folien-Tab in den Vordergrund.
- Falls Demo 2 ohne Aufraeumen abgeschlossen wurde:
  ```sql
  DELETE FROM nessie.curated.curated_esg_emissions
  WHERE source_system = 'manipulation';
  ```

**Demo musste mittendrin abgebrochen werden:**
- `./scripts/demo2-state.sh raw_cur` zurueck zum Demo-Startzustand.
  Idempotent.

**Wiederholung in derselben Session:**
- `./scripts/demo2-state.sh raw_cur` reicht. Dauert ca. 100 Sekunden.

**Demo-1-Pipeline:**
- Bleibt unberuehrt — `raw.fondspreise`, `staging.stg_fondspreise`,
  `curated.snp_fondspreise_scd2` haben ihren Stand aus Demo 1.

---

## Anhang: Befehlsreferenz (Copy-Paste)

### Setup vor der Demo

```bash
docker compose up -d
docker compose ps
./scripts/demo2-state.sh raw_cur
```

### Smoke-Test

```sql
SELECT COUNT(*) FROM nessie.raw.cdp_emissions;                 -- 1
SELECT COUNT(*) FROM nessie.raw.nzdpu_emissions;               -- 1
SELECT COUNT(*) FROM nessie.staging.stg_cdp_emissions;         -- 100
SELECT COUNT(*) FROM nessie.staging.stg_nzdpu_emissions;       -- 90
SELECT COUNT(*) FROM nessie.curated.curated_companies;         -- 30
SELECT COUNT(*) FROM nessie.curated.curated_esg_emissions;     -- 150
```

### Station 2 — Raw beider Quellen

```sql
SELECT source_system, source_version, source_file_format, source_file_size_bytes, source_file_hash
FROM nessie.raw.cdp_emissions
UNION ALL
SELECT source_system, source_version, source_file_format, source_file_size_bytes, source_file_hash
FROM nessie.raw.nzdpu_emissions;
```

### Station 3 — NZDPU-Staging

```sql
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT isin) AS distinct_isins
FROM nessie.staging.stg_nzdpu_emissions;
```

```sql
SELECT isin, reporting_year, scope_1_tco2e, scope_3_total_tco2e
FROM nessie.staging.stg_nzdpu_emissions
WHERE isin = 'DE0007236101'
ORDER BY reporting_year;
```

### Station 4 — CDP-Staging

```sql
SELECT
    COUNT(*)                                          AS rows_total,
    COUNT(*) FILTER (WHERE isin IS NULL)              AS null_isin,
    COUNT(*) FILTER (WHERE scope_1_tco2e IS NULL)     AS null_scope_1,
    COUNT(DISTINCT emission_unit)                     AS distinct_units
FROM nessie.staging.stg_cdp_emissions;
```

### Station 5 — Curated

```sql
SELECT source_system, COUNT(*) FROM nessie.curated.curated_esg_emissions GROUP BY source_system ORDER BY source_system;
SELECT COUNT(*) FROM nessie.curated.curated_companies;
```

### Station 6 — Cross-Source

```sql
SELECT isin, reporting_year, source_system, scope_1_tco2e
FROM nessie.curated.curated_esg_emissions
WHERE isin = 'DE0007236101' AND reporting_year = 2022
ORDER BY source_system;
```

### Station 7 — Gruener Lauf

PowerShell:

```powershell
$env:PYTHONIOENCODING = "utf-8"
uv run python scripts/promote-trusted-esg.py
```

Bash:

```bash
PYTHONIOENCODING=utf-8 uv run python scripts/promote-trusted-esg.py
```

```sql
SELECT COUNT(*) FROM nessie.trusted.trusted_esg_emissions;     -- 150
```

### Station 8 — Roter Lauf (mit Cleanup)

INSERT (Manipulation):

```sql
INSERT INTO nessie.curated.curated_esg_emissions
(isin, reporting_year, source_system, scope_1_tco2e, scope_2_location_tco2e, scope_2_market_tco2e, scope_3_total_tco2e, verification, ingestion_id, ingestion_timestamp, source_file_hash)
VALUES ('DE0007236101', 2099, 'manipulation', DECIMAL '5000000000.000', NULL, NULL, NULL, 'manual_demo_violation', 'demo-violation-uuid', TIMESTAMP '2026-04-27 12:00:00', 'sha256:demo');
```

PowerShell:

```powershell
$env:PYTHONIOENCODING = "utf-8"
uv run python scripts/promote-trusted-esg.py --skip-curated-refresh
```

Bash:

```bash
PYTHONIOENCODING=utf-8 uv run python scripts/promote-trusted-esg.py --skip-curated-refresh
```

Trusted-Check (unveraendert):

```sql
SELECT COUNT(*) FROM nessie.trusted.trusted_esg_emissions;     -- 150
```

Cleanup (Pflicht!):

```sql
DELETE FROM nessie.curated.curated_esg_emissions
WHERE source_system = 'manipulation';
```

### State-Machine fuer Resets

```bash
./scripts/demo2-state.sh raw_cur     # Standard-Demo-Setup
./scripts/demo2-state.sh raw         # nur Raw befuellt (fuer "wie sah Raw aus?"-Fragen)
./scripts/demo2-state.sh empty       # alle Demo-2-Tabellen droppen
./scripts/demo2-state-verify.sh      # Status-Check pro Layer
```

---

## Konkretwerte fuer den Faktencheck

### Pipeline-Counts (Stand `demo2-state.sh raw_cur`)

| Layer / Tabelle                              | Erwartete Rows |
|----------------------------------------------|----------------|
| `raw.cdp_emissions`                          | 1              |
| `raw.nzdpu_emissions`                        | 1              |
| `staging.stg_cdp_emissions`                  | 100            |
| `staging.stg_nzdpu_emissions`                | 90             |
| `curated.curated_companies`                  | 30             |
| `curated.curated_esg_emissions`              | 150            |
| `trusted.trusted_esg_emissions` (vor Demo)   | (existiert nicht) |
| `trusted.trusted_esg_emissions` (nach Stat. 7) | 150          |
| `trusted.trusted_esg_emissions` (nach Stat. 8) | 150 (unveraendert) |

### Datenqualitaets-Befunde (CDP)

- 40 / 100 Records mit `ISIN IS NULL`
- 11 / 100 Records mit `Scope 1 IS NULL`
- 3 distinct `emission_unit`-Werte (`metric tons CO2e`, `tonnes CO2`, NULL)

### Trusted-Schema-Test (didaktisch)

- `dbt test --select trusted_esg_emissions` zeigt 6 Failures auf
  `not_null_trusted_esg_emissions_scope_1_tco2e`. Das ist gewollt:
  Curated laesst NULL-`scope_1` zu, Trusted-Schema nicht. In der
  Live-Demo nicht zwingend zeigen — nur Backup-Material fuer
  Nachfragen.

### Cross-Source-Beispiel

- ISIN: `DE0007236101` (Siemens AG)
- Berichtsjahr: 2022
- NZDPU `scope_1_tco2e`: `3 794 435.000`
- CDP   `scope_1_tco2e`: `3 253 366.000`
- Differenz: ca. `541 069` tCO2e (~ 17 Prozent)

### Quality-Gate-Suite (6 Expectations)

1. `expect_table_row_count_to_be_between` — 100 bis 200
2. `expect_column_values_to_not_be_null` — `isin`
3. `expect_column_value_lengths_to_equal` — `isin = 12`
4. `expect_column_values_to_be_in_set` — `source_system in ('nzdpu','cdp')`
5. `expect_column_values_to_be_between` — `scope_1_tco2e` 0..100M, mostly 0.95
6. `expect_compound_columns_to_be_unique` — `(isin, reporting_year, source_system)`

Manipulation in Station 8 verletzt #4 (`source_system = 'manipulation'`)
und #2/#3 fuer die ISIN-Plausibilitaet — die Suite faellt mit
mindestens einer FAIL-Zeile.

---

## Troubleshooting-Schnellreferenz

| Symptom                                           | Schneller Fix                                                       |
|---------------------------------------------------|---------------------------------------------------------------------|
| Eine Demo-2-Tabelle leer / nicht da               | `./scripts/demo2-state.sh raw_cur`                                  |
| `UnicodeEncodeError` im PowerShell-Aufruf         | `$env:PYTHONIOENCODING = "utf-8"` voranstellen                      |
| Skript laeuft Phase 3 obwohl nicht erwartet       | `--skip-curated-refresh` vergessen — Manipulation ist weg, INSERT erneut |
| Phase 2 ploetzlich rot ohne Manipulation          | Manipulations-INSERT aus Station 8 nicht aufgeraeumt — Cleanup-DELETE   |
| GE-Checkpoint Exception "table does not exist"    | Curated nicht gebaut — `./scripts/demo2-state.sh raw_cur`           |
| `docker compose exec: not running`                | Stack hochfahren mit `docker compose up -d`                         |
| Cross-Source-Query nur 1 Zeile                    | ISIN nicht in beiden Quellen — andere DAX-ISIN waehlen oder Reset   |
| Browser zeigt Data-Docs nicht                     | Pfad pruefen, Forward-Slashes verwenden, ggf. zweiten Skript-Lauf   |
