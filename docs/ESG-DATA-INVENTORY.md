# ESG Sample Data Inventory

Stand: 2026-04-26
Zweck: Bestandsaufnahme der ESG-Sample-Daten in mini-lakehouse als
Ausgangsbasis fuer den Demo-2-Pipeline-Umbau (AP-8: CDP-Ingestion,
AP-9: NZDPU-Ingestion).

## Sample-Dateien im Ueberblick

| Datei                              | Demo 2                  | WS1/WS2                                        |
|------------------------------------|-------------------------|------------------------------------------------|
| data/sample/nzdpu_emissions.json   | JA (Quelle fuer Demo 2) | 01_iceberg_erkunden, 02_time_travel_schema_evolution |
| data/sample/nzdpu_emissions_v2.json| NEIN                    | 02_time_travel_schema_evolution                |
| data/sample/cdp_emissions.csv      | JA (Quelle fuer Demo 2) | 01_iceberg_erkunden, spark_init.py             |

`test_real_esg_api.ipynb` referenziert keine der drei Dateien.

## Detail-Inventur

### data/sample/nzdpu_emissions.json

- Groesse: 75 491 Bytes
- Top-Level-Struktur: JSON-Objekt (Wrapper)
  - `status`: string ("ok")
  - `total_records`: int (30) — bezieht sich auf Companies, NICHT auf
    flat records
  - `source`: string ("nzdpu_fallback")
  - `data`: list[Company]
- Companies im `data`-Array: 30
- Reporting-Periods insgesamt (nach Flatten): 90 (30 Companies x 3 Jahre)
- Jahre: 2021, 2022, 2023
- Eindeutige ISINs: 30
- Encoding-Hinweis: `_meta.note` enthaelt mojibake-artige Sequenzen
  (`fÃ¼r` statt `fuer`, `â€”` statt em-dash) —
  die Datei wurde mit fehlerhafter Encoding-Pipeline erzeugt. Fuer den
  Demo-2-Lauf irrelevant, da `_meta` nicht ins Staging propagiert wird.

#### Schema (pro Company-Record)

| Feld                       | Typ              | Beispiel                       |
|----------------------------|------------------|--------------------------------|
| `company_id`               | string           | "NZDPU-0001"                   |
| `company_name`             | string           | "Siemens AG"                   |
| `isin`                     | string (12)      | "DE0007236101"                 |
| `lei`                      | string (20)      | "549300DE00072361"             |
| `country_of_incorporation` | string           | "Germany"                      |
| `primary_sector`           | string           | "Industrials"                  |
| `reporting_periods`        | list[Period]     | s. unten                       |
| `_meta`                    | dict             | source/retrieved/note          |

#### Schema (pro Reporting-Period)

| Feld                     | Typ                 | Beispiel               |
|--------------------------|---------------------|------------------------|
| `reporting_year`         | int                 | 2023                   |
| `reporting_framework`    | string \| null      | "GHG Protocol", null   |
| `verification_status`    | string              | "third_party_verified" |
| `scope_1`                | dict {value, unit}  | {3002863, "tCO2e"}     |
| `scope_2_location_based` | dict \| null        | {439768, "tCO2e"}      |
| `scope_2_market_based`   | dict \| null        | {378180, "tCO2e"}      |
| `scope_3`                | dict \| null        | s. Beispiel-Record     |
| `net_zero_target`        | dict \| null        | s. Beispiel-Record     |

Anteil Null-Felder (90 Periods):
- `scope_3 = null`: 35 von 90 (39 %)
- `net_zero_target = null`: 64 von 90 (71 %)

#### Beispiel-Record (Auszug, 2023-Period)

```json
{
  "company_id": "NZDPU-0001",
  "company_name": "Siemens AG",
  "isin": "DE0007236101",
  "lei": "549300DE00072361",
  "country_of_incorporation": "Germany",
  "primary_sector": "Industrials",
  "reporting_periods": [
    {
      "reporting_year": 2023,
      "reporting_framework": "ISO 14064",
      "verification_status": "third_party_verified",
      "scope_1": { "value": 3002863, "unit": "tCO2e" },
      "scope_2_location_based": { "value": 439768, "unit": "tCO2e" },
      "scope_2_market_based": { "value": 378180, "unit": "tCO2e" },
      "scope_3": {
        "total": 28253375,
        "categories_reported": 13,
        "unit": "tCO2e",
        "data_quality": "calculated"
      },
      "net_zero_target": {
        "type": "absolute",
        "base_year": 2019,
        "target_year": 2050,
        "reduction_pct": 52,
        "sbti_validated": false
      }
    }
  ]
}
```

### data/sample/nzdpu_emissions_v2.json

- Groesse: 60 344 Bytes
- Top-Level-Struktur: JSON-Array (kein Wrapper)
- Records: 90 (bereits flach, eine Zeile pro (Company, Year))
- Eindeutige ISINs: 30 — identisch mit v1
- Jahre: 2021, 2022, 2023 — identisch mit v1
- (`isin`, `reporting_year`) eindeutig: ja
- `metadata.api_version`: "v2" (durchgaengig)

#### Schema (pro Record, flach mit eingebetteten Sub-Strukturen)

| Feld                                | Typ      |
|-------------------------------------|----------|
| `entity.id`                         | string   |
| `entity.name`                       | string   |
| `entity.isin`                       | string   |
| `entity.lei`                        | string   |
| `entity.country`                    | string   |
| `industry_classification`           | string   |
| `reporting_year`                    | int      |
| `emissions.scope_1_tco2e`           | int/null |
| `emissions.scope_2_location_tco2e`  | int/null |
| `emissions.scope_2_market_tco2e`    | int/null |
| `emissions.scope_3_total_tco2e`     | int/null |
| `emissions.unit`                    | string   |
| `climate_target.net_zero_year`      | int/null |
| `metadata.reporting_framework`      | string/null |
| `metadata.verification`             | string   |
| `metadata.api_version`              | string ("v2") |

#### Beispiel-Record

```json
{
  "entity": {
    "id": "NZDPU-0001",
    "name": "Siemens AG",
    "isin": "DE0007236101",
    "lei": "549300DE00072361",
    "country": "Germany"
  },
  "industry_classification": "Industrials",
  "reporting_year": 2021,
  "emissions": {
    "scope_1_tco2e": 3430896,
    "scope_2_location_tco2e": 381067,
    "scope_2_market_tco2e": 270562,
    "scope_3_total_tco2e": null,
    "unit": "tCO2e"
  },
  "climate_target": { "net_zero_year": null },
  "metadata": {
    "reporting_framework": null,
    "verification": "third_party_verified",
    "api_version": "v2"
  }
}
```

#### Unterschied v1 -> v2

| Aspekt                | v1                                           | v2                                                |
|-----------------------|----------------------------------------------|---------------------------------------------------|
| Top-Level             | Wrapper-Objekt mit `status`/`total_records`/`source`/`data` | nackter JSON-Array                              |
| Granularitaet         | Company mit verschachteltem `reporting_periods` | bereits flach: ein Record pro (Company, Jahr) |
| Scope-Werte           | `scope_1.value` + `scope_1.unit`             | `emissions.scope_1_tco2e` (Wert direkt, Unit zentral in `emissions.unit`) |
| Klima-Ziel            | `net_zero_target` als Dict mit base_year/target_year/reduction_pct/sbti_validated | reduziert auf `climate_target.net_zero_year` (int oder null) |
| Scope-3-Detail        | `scope_3` als Dict mit categories_reported/data_quality | nur `emissions.scope_3_total_tco2e`             |
| Verification          | `verification_status` (Enum-String) am Period-Level | `metadata.verification` am Record-Level        |
| Sektor-Feld           | `primary_sector`                             | `industry_classification`                         |
| API-Version           | implizit (Wrapper.source = "nzdpu_fallback") | explizit (`metadata.api_version = "v2"`)         |

Daten sind inhaltlich identisch (gleiche 30 ISINs, gleiche Jahre,
gleiche Scope-1-Werte). v2 ist eine reine Schema-Variante.

Hinweis: nzdpu_emissions_v2.json wird NICHT fuer Demo 2 verwendet.
Sie bleibt im Repo, weil 02_time_travel_schema_evolution.ipynb sie
fuer die Schema-Evolution-Demo (v1-Schema -> v2-Schema mit
`ALTER TABLE ADD COLUMNS`) referenziert. Kollegen, die WS1/WS2 nach
einem Demo-1-Reset eigenstaendig durchspielen, brauchen v2 dort
weiterhin.

### data/sample/cdp_emissions.csv

- Groesse: 14 973 Bytes
- Datenzeilen: 100 (ohne Header)
- Eindeutige ISINs (inkl. Leerwerte): 31
- Eindeutige ISINs (ohne Leerwerte): 30
- Jahre: 2022, 2023
- Datenqualitaets-Auffaelligkeiten:
  - 40 von 100 Zeilen haben leere `ISIN`-Spalte
  - 11 von 100 Zeilen haben leeres `Scope 1`
  - `Emission Unit` mixed: "metric tons CO2e" und "tonnes CO2"
  - `Primary Sector` inkonsistent: 17 verschiedene Auspraegungen,
    teilweise redundant (Industrials vs. Automobiles, Materials vs.
    Chemicals, Health Care vs. Pharmaceuticals, Technology vs.
    Information Technology, Financials vs. Financial Services,
    Communication Services vs. Telecommunications)

#### Header

```
Account Number, Organization, Primary Sector, Primary Industry,
Country, ISIN, Reporting Year, Scope 1 (metric tons CO2e),
Scope 2 Location-Based (metric tons CO2e),
Scope 2 Market-Based (metric tons CO2e),
Scope 3 Total (metric tons CO2e), Emission Unit, Data Verification,
CDP Score, Public Disclosure
```

#### Datentypen pro Spalte

| Spalte                        | Typ              | Bemerkung                       |
|-------------------------------|------------------|---------------------------------|
| Account Number                | string           |                                 |
| Organization                  | string           |                                 |
| Primary Sector                | string           | inkonsistente Werte             |
| Primary Industry              | string           |                                 |
| Country                       | string           |                                 |
| ISIN                          | string \| empty  | 40 leer                         |
| Reporting Year                | int              | nur 2022, 2023                  |
| Scope 1 (metric tons CO2e)    | float \| empty   | 11 leer; min 65 841, max 51 044 782 |
| Scope 2 Location-Based ...    | float \| empty   |                                 |
| Scope 2 Market-Based ...      | float \| empty   |                                 |
| Scope 3 Total ...             | float \| empty   | viele leer                      |
| Emission Unit                 | string \| empty  | "metric tons CO2e" / "tonnes CO2" |
| Data Verification             | string \| empty  | not/limited/third-party verified |
| CDP Score                     | string           | A, A-, B, B-, C, ...            |
| Public Disclosure             | string           | "Yes" (58) / "No" (42)          |

#### Beispiel-Zeile

```
CDP-DE0007236101-2022,Siemens AG,Industrials,Industrials,Germany,
DE0007236101,2022,3253366.0,464767.0,254258.0,64627392.0,
metric tons CO2e,Not verified,A,Yes
```

## Beurteilung Demo-2-Tauglichkeit

Bewertung der Demo-2-Quellen (`nzdpu_emissions.json`, `cdp_emissions.csv`)
gemaess Kriterien A-D.

### Datenmenge (Kriterium A)

- NZDPU: 30 Companies / 90 flat-records nach JSON-Expand. >= 30. OK.
- CDP: 100 Zeilen. >= 30 und < 1000. OK.
- Kein Performance-Risiko fuer Live-Demo (zusammen ca. 200 Records).

### Datenvielfalt (Kriterium B)

- 30 ISINs in NZDPU, 30 nicht-leere ISINs in CDP. >= 5. OK.
- Schnittmenge: 30 von 30 — **100 % Ueberschneidung der nicht-leeren
  ISINs**. Das ist ideal fuer eine UNION-Harmonisierungs-Demo in
  Curated: pro Unternehmen entstehen Records aus beiden Quellen, der
  Source-of-Truth-Konflikt wird sichtbar.

### Fachliche Plausibilitaet (Kriterium C)

- ISIN-Format: 12-stellig, alphanumerisch (CH/DE-Praefixe), realistisch.
- `scope_1` Range NZDPU: 68 353 – 48 241 206 tCO2e — passt zu DAX-
  Konzernen.
- `scope_1` Range CDP: 65 841 – 51 044 782 tCO2e — konsistent.
- Keine negativen Werte, keine offensichtlich unsinnigen Werte.
- Berichtsjahre 2021-2023 (NZDPU) bzw. 2022-2023 (CDP). Realistisch.

### Quality-Gate-Eignung (Kriterium D)

Die Daten enthalten mehrere absichtliche Edge Cases, die fuer
Great-Expectations-Demos in Demo 2 ausgenutzt werden koennen:

- **Eindeutigkeit (`expect_compound_columns_to_be_unique`)**:
  - NZDPU nach Flatten: (`isin`, `reporting_year`) eindeutig in allen
    90 Records. Test gruen.
  - CDP nicht-leere ISINs: (`ISIN`, `Reporting Year`) eindeutig in allen
    60 Zeilen. Test gruen.
  - CDP gesamt: 40 Zeilen mit leerem ISIN bilden 40 Duplikat-Tupel
    (leer, 2022/2023). Test rot — sichtbarer Quality-Gate-Fail im
    Demo, ohne dass Daten manipuliert werden muessen.

- **Not-Null (`expect_column_values_to_not_be_null`)**:
  - CDP `ISIN`: 40 % null. Schon ohne Live-Modifikation rot.
  - CDP `Scope 1`: 11 % null. Demonstriert weichen Threshold.

- **Wertebereich (`expect_column_values_to_be_between`)**:
  - CDP `Scope 1` max bei 51 044 782 tCO2e — wenn ein 60-Mio-tCO2e-
    Plausibilitaets-Limit gesetzt wird, ist eine bewusste Live-
    Aenderung (`* 10`) sofort sichtbar.

- **Set-Membership (`expect_column_values_to_be_in_set`)**:
  - CDP `Emission Unit` mixed ("metric tons CO2e" / "tonnes CO2") —
    zeigt Harmonisierungs-Bedarf.
  - CDP `Primary Sector` 17 inkonsistente Auspraegungen — zeigt
    Mapping-Bedarf in Curated.

### Fazit

Die bestehenden Sample-Daten erfuellen alle vier Kriterien und sind
fuer Demo 2 ausreichend. **Es wird KEIN Generator gebaut.** Die
Dateien bleiben unveraendert.

## Aktionen fuer AP-7

- BEHALTEN: `data/sample/nzdpu_emissions_v2.json` (fuer
  WS1/WS2-Replizierbarkeit durch Kollegen — von
  02_time_travel_schema_evolution.ipynb referenziert)
- UNVERAENDERT: `data/sample/nzdpu_emissions.json`
- UNVERAENDERT: `data/sample/cdp_emissions.csv`
- KEIN Generator-Skript: `scripts/generate-esg-samples.py` wurde
  nicht erstellt, da Sample-Daten gemaess Kriterien A-D ausreichend
  sind.

## Schemas fuer AP-8 und AP-9

### CDP Schema (CSV) — Eingabe fuer AP-8

CSV mit 15 Spalten, UTF-8, Komma-getrennt, Header-Zeile vorhanden.

Pflichtspalten fuer Demo-2-Pipeline:
- `Account Number` (string) — fachlicher Identifier (Wert-Form
  `CDP-<ISIN>-<Year>`)
- `ISIN` (string, 12 Zeichen, kann leer sein) — fachlicher Schluessel
  fuer Curated-UNION
- `Reporting Year` (int) — fachlicher Schluessel
- `Scope 1 (metric tons CO2e)` (float, kann leer sein)
- `Scope 2 Location-Based (metric tons CO2e)` (float, kann leer sein)
- `Scope 2 Market-Based (metric tons CO2e)` (float, kann leer sein)
- `Scope 3 Total (metric tons CO2e)` (float, kann leer sein)

Begleitspalten (Curated/Trusted optional):
- `Organization`, `Primary Sector`, `Primary Industry`, `Country`,
  `Emission Unit`, `Data Verification`, `CDP Score`, `Public Disclosure`

Raw-Schicht-Erwartung: 1 Row pro CSV-Zeile + File-level Provenance
(`source_file`, `loaded_at`, `row_number`).

### NZDPU Schema (JSON) — Eingabe fuer AP-9

JSON mit Wrapper-Objekt am Top-Level.

Wrapper-Felder:
- `status` (string)
- `total_records` (int) — bezieht sich auf Companies, NICHT auf flat
  records
- `source` (string)
- `data` (list[Company])

Company-Felder (an JSON-Expand-Stage interessant):
- `company_id`, `company_name`, `isin`, `lei`,
  `country_of_incorporation`, `primary_sector`
- `reporting_periods`: list[Period] — muss in Staging via `EXPLODE`
  geflattet werden
- `_meta`: dict — fuer Provenance ggf. interessant
  (`_meta.source`, `_meta.retrieved`)

Period-Felder (nach Flatten):
- `reporting_year` (int)
- `reporting_framework` (string|null)
- `verification_status` (string)
- `scope_1.value` (int), `scope_1.unit` (string)
- `scope_2_location_based.value`, `scope_2_location_based.unit`
- `scope_2_market_based.value`, `scope_2_market_based.unit`
- `scope_3` (dict|null) mit `total`, `categories_reported`, `unit`,
  `data_quality`
- `net_zero_target` (dict|null) mit `type`, `base_year`,
  `target_year`, `reduction_pct`, `sbti_validated`

Raw-Schicht-Erwartung: 1 Row pro Eingangsdatei (Datei-als-Blob) plus
File-level Provenance (`source_file`, `loaded_at`, `row_count`).

Staging-Erwartung: JSON-Expand entlang `data[*].reporting_periods[*]`
ergibt 90 Rows pro Datei (30 Companies x 3 Jahre). Pro Row dann je
ein Scope-Wert flach.
