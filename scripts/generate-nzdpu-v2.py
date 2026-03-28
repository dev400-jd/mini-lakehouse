"""
generate-nzdpu-v2.py — Erzeugt data/sample/nzdpu_emissions_v2.json

Liest die bestehende V1-Datei (nzdpu_emissions.json) und transformiert sie
in das neue API-v2-Format: verschachtelte Struktur, umbenannte Felder,
neue Felder (scope_3, net_zero_year).

Ausführung: uv run scripts/generate-nzdpu-v2.py
         OR: python scripts/generate-nzdpu-v2.py

Was sich strukturell ändert:
  V1: { "data": [ { company_name, reporting_periods: [...] } ] }
      → pro Unternehmen mit Liste von Berichtsjahren

  V2: [ { entity: {...}, industry_classification, reporting_year,
          emissions: {...}, climate_target: {...}, metadata: {...} } ]
      → flaches Array, eine Zeile pro Unternehmen × Berichtsjahr

Feld-Mappings:
  company_name            → entity.name
  country_of_incorporation→ entity.country
  primary_sector          → industry_classification
  scope_1.value           → emissions.scope_1_tco2e
  scope_2_location_based  → emissions.scope_2_location_tco2e
  scope_3.total           → emissions.scope_3_total_tco2e  (neu!)
  net_zero_target.year    → climate_target.net_zero_year   (neu!)
  verification_status     → metadata.verification
"""

import json
import sys
from pathlib import Path

# UTF-8 output
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

REPO_ROOT = Path(__file__).parent.parent
V1_FILE = REPO_ROOT / "data" / "sample" / "nzdpu_emissions.json"
V2_FILE = REPO_ROOT / "data" / "sample" / "nzdpu_emissions_v2.json"


def transform(v1: dict) -> list:
    """Transformiert die V1-API-Response in das V2-Format (flaches Array)."""
    records = []

    for company in v1["data"]:
        entity = {
            "id":      company["company_id"],
            "name":    company["company_name"],
            "isin":    company["isin"],
            "lei":     company["lei"],
            "country": company["country_of_incorporation"],
        }
        industry = company["primary_sector"]

        for period in company["reporting_periods"]:
            # Scope-Werte: V1 hatte {"value": X, "unit": "tCO2e"}, V2 hat flache Zahlen
            s1 = period["scope_1"]["value"] if period.get("scope_1") else None
            s2_loc = (period["scope_2_location_based"]["value"]
                      if period.get("scope_2_location_based") else None)
            s2_mkt = (period["scope_2_market_based"]["value"]
                      if period.get("scope_2_market_based") else None)
            s3 = (period["scope_3"]["total"]
                  if period.get("scope_3") else None)

            # Net-Zero-Ziel: V1 hatte ganzes Ziel-Objekt, V2 nur das Jahr
            nzt = period.get("net_zero_target")
            net_zero_year = nzt["target_year"] if nzt else None

            records.append({
                "entity":                  entity,
                "industry_classification": industry,
                "reporting_year":          period["reporting_year"],
                "emissions": {
                    "scope_1_tco2e":          s1,
                    "scope_2_location_tco2e": s2_loc,
                    "scope_2_market_tco2e":   s2_mkt,
                    "scope_3_total_tco2e":    s3,
                    "unit":                   "tCO2e",
                },
                "climate_target": {
                    "net_zero_year": net_zero_year,
                },
                "metadata": {
                    "reporting_framework": period.get("reporting_framework"),
                    "verification":        period.get("verification_status"),
                    "api_version":         "v2",
                },
            })

    return records


def main():
    if not V1_FILE.exists():
        print(f"Fehler: {V1_FILE} nicht gefunden — erst 'make seed' oder generate-sample-data.py ausführen")
        sys.exit(1)

    with open(V1_FILE, encoding="utf-8") as f:
        v1 = json.load(f)

    records = transform(v1)

    V2_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(V2_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    print(f"nzdpu_emissions_v2.json generiert: {len(records)} Einträge, verschachtelte Struktur")
    print(f"Pfad: {V2_FILE}")


if __name__ == "__main__":
    main()
