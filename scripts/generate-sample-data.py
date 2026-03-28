"""
generate-sample-data.py - Lädt ESG-Beispieldaten aus öffentlichen Quellen.

Quellen:
  1. NZDPU API          -> data/sample/nzdpu_emissions.json   (API-Key optional)
  2. CDP Open Data      -> data/sample/cdp_emissions.csv      (Socrata API)
  3. Our World in Data  -> data/sample/owid_co2_countries.csv (GitHub)
  4. Synthetische Fonds -> data/sample/fund_master.csv
                          data/sample/fund_positions.csv

Ausfuehrung: uv run scripts/generate-sample-data.py
NZDPU API-Key (optional): export NZDPU_API_TOKEN=<token>
"""

import sys
import io
# Force UTF-8 output on Windows
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8")

import json
import os
import random
import time
from datetime import date
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Konfiguration
# ---------------------------------------------------------------------------

OUTPUT_DIR = Path(__file__).parent.parent / "data" / "sample"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

TIMEOUT = 20  # Sekunden pro Request

# Europäische Unternehmen mit realen ISINs (DAX, EURO STOXX 50, FTSE, SMI)
EU_COMPANIES = [
    {"name": "Siemens AG",                          "isin": "DE0007236101", "country": "Germany",        "sector": "Industrials"},
    {"name": "BASF SE",                             "isin": "DE000BASF111", "country": "Germany",        "sector": "Materials"},
    {"name": "Allianz SE",                          "isin": "DE0008404005", "country": "Germany",        "sector": "Financials"},
    {"name": "SAP SE",                              "isin": "DE0007164600", "country": "Germany",        "sector": "Information Technology"},
    {"name": "Deutsche Telekom AG",                 "isin": "DE0005557508", "country": "Germany",        "sector": "Communication Services"},
    {"name": "BMW AG",                              "isin": "DE0005190003", "country": "Germany",        "sector": "Consumer Discretionary"},
    {"name": "Bayer AG",                            "isin": "DE000BAY0017", "country": "Germany",        "sector": "Health Care"},
    {"name": "Deutsche Bank AG",                    "isin": "DE0005140008", "country": "Germany",        "sector": "Financials"},
    {"name": "Volkswagen AG",                       "isin": "DE0007664039", "country": "Germany",        "sector": "Consumer Discretionary"},
    {"name": "Mercedes-Benz Group AG",              "isin": "DE0007100000", "country": "Germany",        "sector": "Consumer Discretionary"},
    {"name": "Infineon Technologies AG",            "isin": "DE0006231004", "country": "Germany",        "sector": "Information Technology"},
    {"name": "Muenchener Rueckversicherung AG",     "isin": "DE0008430026", "country": "Germany",        "sector": "Financials"},
    {"name": "adidas AG",                           "isin": "DE000A1EWWW0", "country": "Germany",        "sector": "Consumer Discretionary"},
    {"name": "Linde plc",                           "isin": "IE00BZ12WP82", "country": "Ireland",        "sector": "Materials"},
    {"name": "Airbus SE",                           "isin": "NL0000235190", "country": "Netherlands",    "sector": "Industrials"},
    {"name": "ASML Holding NV",                     "isin": "NL0010273215", "country": "Netherlands",    "sector": "Information Technology"},
    {"name": "LVMH Moet Hennessy Louis Vuitton SE", "isin": "FR0000121014", "country": "France",         "sector": "Consumer Discretionary"},
    {"name": "TotalEnergies SE",                    "isin": "FR0014000MR3", "country": "France",         "sector": "Energy"},
    {"name": "BNP Paribas SA",                      "isin": "FR0000131104", "country": "France",         "sector": "Financials"},
    {"name": "Schneider Electric SE",               "isin": "FR0000121972", "country": "France",         "sector": "Industrials"},
    {"name": "Sanofi SA",                           "isin": "FR0000120578", "country": "France",         "sector": "Health Care"},
    {"name": "Shell plc",                           "isin": "GB00BP6MXD84", "country": "United Kingdom", "sector": "Energy"},
    {"name": "AstraZeneca PLC",                     "isin": "GB0009895292", "country": "United Kingdom", "sector": "Health Care"},
    {"name": "HSBC Holdings plc",                   "isin": "GB0005405286", "country": "United Kingdom", "sector": "Financials"},
    {"name": "BP p.l.c.",                           "isin": "GB0007980591", "country": "United Kingdom", "sector": "Energy"},
    {"name": "Unilever PLC",                        "isin": "GB00B10RZP78", "country": "United Kingdom", "sector": "Consumer Staples"},
    {"name": "Nestle S.A.",                         "isin": "CH0038863350", "country": "Switzerland",    "sector": "Consumer Staples"},
    {"name": "Novartis AG",                         "isin": "CH0012221716", "country": "Switzerland",    "sector": "Health Care"},
    {"name": "Roche Holding AG",                    "isin": "CH0012032048", "country": "Switzerland",    "sector": "Health Care"},
    {"name": "Zurich Insurance Group AG",           "isin": "CH0011075394", "country": "Switzerland",    "sector": "Financials"},
]

# Emissionsprofile pro Sektor (Scope 1 Basiswert in tCO2e, Faktor für Streuung)
SECTOR_EMISSIONS = {
    "Energy":                    {"s1_base": 45_000_000, "s2_base": 800_000},
    "Materials":                 {"s1_base": 12_000_000, "s2_base": 1_200_000},
    "Industrials":               {"s1_base":  3_500_000, "s2_base":   500_000},
    "Consumer Discretionary":    {"s1_base":  4_000_000, "s2_base":   400_000},
    "Consumer Staples":          {"s1_base":  1_500_000, "s2_base":   300_000},
    "Health Care":               {"s1_base":    800_000, "s2_base":   200_000},
    "Financials":                {"s1_base":     80_000, "s2_base":    60_000},
    "Information Technology":    {"s1_base":    120_000, "s2_base":   400_000},
    "Communication Services":    {"s1_base":    600_000, "s2_base":   900_000},
}

CREATED_FILES: list[dict] = []

# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    print(f"  {msg}")


def record_file(path: Path) -> None:
    size_kb = path.stat().st_size / 1024
    # Zeilenanzahl für CSV/JSON schätzen
    try:
        lines = path.read_text(encoding="utf-8").count("\n")
    except Exception:
        lines = -1
    CREATED_FILES.append({"Datei": path.name, "Zeilen": lines, "Größe (KB)": round(size_kb, 1)})


# ---------------------------------------------------------------------------
# Quelle 1: NZDPU API
# ---------------------------------------------------------------------------

def _nzdpu_fallback() -> dict:
    """
    Fallback: Realistische NZDPU-ähnliche API-Response für 30 europäische Unternehmen.
    Struktur entspricht dem typischen Format einer ESG-Daten-API:
    Nested, uneinheitlich, mit fehlenden Werten — genau wie in der Realität.
    """
    random.seed(42)
    records = []
    for i, co in enumerate(EU_COMPANIES):
        profile = SECTOR_EMISSIONS.get(co["sector"], {"s1_base": 500_000, "s2_base": 200_000})
        periods = []
        for year in [2021, 2022, 2023]:
            # Trend: jährliche Reduktion ~3%
            factor = (1 - 0.03) ** (2023 - year)
            s1 = round(profile["s1_base"] * factor * random.uniform(0.85, 1.15))
            s2_loc = round(profile["s2_base"] * factor * random.uniform(0.8, 1.2))
            s2_mkt = round(s2_loc * random.uniform(0.6, 1.0))  # oft kleiner als location-based

            # Absichtlich unvollständige Daten für manche Unternehmen/Jahre
            scope3 = None
            if random.random() > 0.35:
                scope3 = {
                    "total": round(s1 * random.uniform(5, 25)),
                    "categories_reported": random.randint(4, 15),
                    "unit": "tCO2e",
                    "data_quality": random.choice(["estimated", "calculated", "verified"]),
                }

            target = None
            if year == 2023 and random.random() > 0.3:
                target = {
                    "type": random.choice(["absolute", "intensity"]),
                    "base_year": random.choice([2019, 2020, 2021]),
                    "target_year": random.choice([2030, 2035, 2050]),
                    "reduction_pct": random.randint(25, 65),
                    "sbti_validated": random.choice([True, False]),
                }

            periods.append({
                "reporting_year": year,
                "reporting_framework": random.choice(["GHG Protocol", "ISO 14064", None]),
                "verification_status": random.choice(["third_party_verified", "limited_assurance", "not_verified"]),
                "scope_1": {"value": s1, "unit": "tCO2e"},
                "scope_2_location_based": {"value": s2_loc, "unit": "tCO2e"},
                "scope_2_market_based": {"value": s2_mkt, "unit": "tCO2e"} if random.random() > 0.2 else None,
                "scope_3": scope3,
                "net_zero_target": target,
            })

        records.append({
            "company_id": f"NZDPU-{i+1:04d}",
            "company_name": co["name"],
            "isin": co["isin"],
            "lei": f"549300{co['isin'][:10]}",  # fiktive LEI, reales Präfix-Muster
            "country_of_incorporation": co["country"],
            "primary_sector": co["sector"],
            "reporting_periods": periods,
            "_meta": {
                "source": "NZDPU (Net Zero Data Public Utility)",
                "retrieved": "2024-03-01",
                "note": "Fallback-Daten — für echte API: NZDPU_API_TOKEN setzen",
            },
        })

    return {
        "status": "ok",
        "total_records": len(records),
        "source": "nzdpu_fallback",
        "data": records,
    }


def fetch_nzdpu() -> None:
    """
    Versucht den NZDPU API-Endpoint. Erfordert einen kostenlosen API-Key
    (Registrierung unter https://nzdpu.com). Ohne Token: Fallback auf
    statische Daten die die gleiche Nested-JSON-Struktur spiegeln.
    """
    print("\n[1/4] NZDPU Emissions API ->", OUTPUT_DIR / "nzdpu_emissions.json")
    token = os.environ.get("NZDPU_API_TOKEN")
    out = OUTPUT_DIR / "nzdpu_emissions.json"

    if token:
        try:
            headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
            # NZDPU company search — Endpoint laut offizieller Doku
            url = "https://api.nzdpu.com/v1/companies"
            params = {"limit": 50, "country": "DE,FR,GB,CH,NL,IE"}
            log(f"API-Request: GET {url}")
            resp = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
            resp.raise_for_status()
            payload = resp.json()
            out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
            log(f"✓ API-Response gespeichert ({len(payload.get('data', []))} Unternehmen)")
            record_file(out)
            return
        except requests.RequestException as e:
            log(f"⚠ API nicht erreichbar ({e}) — verwende Fallback-Daten")
    else:
        log("ℹ Kein NZDPU_API_TOKEN gesetzt — verwende Fallback-Daten")
        log("  (Kostenlosen API-Key unter https://nzdpu.com/api-docs anfordern)")

    payload = _nzdpu_fallback()
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    log(f"✓ Fallback gespeichert ({payload['total_records']} Unternehmen, nested JSON)")
    record_file(out)


# ---------------------------------------------------------------------------
# Quelle 2: CDP Open Data
# ---------------------------------------------------------------------------

def _cdp_fallback_df() -> pd.DataFrame:
    """
    Fallback: Realistisches CDP-ähnliches CSV mit echten Datenqualitätsproblemen.
    Spalten entsprechen dem CDP Climate Change Questionnaire-Datensatz.
    """
    random.seed(7)
    rows = []
    eu_countries = ["Germany", "France", "United Kingdom", "Netherlands",
                    "Switzerland", "Sweden", "Spain", "Italy", "Denmark", "Norway"]
    sectors = ["Financial Services", "Chemicals", "Utilities", "Automobiles",
               "Pharmaceuticals", "Technology", "Energy", "Retail", "Telecommunications"]

    for co in EU_COMPANIES:
        for year in [2022, 2023]:
            profile = SECTOR_EMISSIONS.get(co["sector"], {"s1_base": 500_000, "s2_base": 200_000})
            factor = random.uniform(0.8, 1.2)
            # Absichtlich gemischte Lücken und Einheitenprobleme — echte CDP-Daten
            scope1 = round(profile["s1_base"] * factor) if random.random() > 0.1 else None
            scope2_loc = round(profile["s2_base"] * factor) if random.random() > 0.15 else None
            # Manche Unternehmen melden in unterschiedlichen Einheiten
            unit = random.choice(["metric tons CO2e", "metric tons CO2e", "metric tons CO2e", "tonnes CO2"])
            rows.append({
                "Account Number": f"CDP-{co['isin']}-{year}",
                "Organization": co["name"],
                "Primary Sector": co["sector"],
                "Primary Industry": co["sector"],
                "Country": co["country"],
                "ISIN": co["isin"],
                "Reporting Year": year,
                "Scope 1 (metric tons CO2e)": scope1,
                "Scope 2 Location-Based (metric tons CO2e)": scope2_loc,
                "Scope 2 Market-Based (metric tons CO2e)": round(scope2_loc * random.uniform(0.5, 1.0)) if scope2_loc and random.random() > 0.25 else None,
                "Scope 3 Total (metric tons CO2e)": round(profile["s1_base"] * random.uniform(3, 20)) if random.random() > 0.4 else None,
                "Emission Unit": unit,
                "Data Verification": random.choice(["Third-party verified", "Not verified", "Limited assurance", ""]),
                "CDP Score": random.choice(["A", "A-", "B", "B-", "C", "D", None]),
                "Public Disclosure": random.choice(["Yes", "No"]),
            })

    # Zusätzliche Rauschen-Zeilen: weitere europäische Unternehmen ohne ISIN (wie in echten CDP-Daten)
    for i in range(40):
        country = random.choice(eu_countries)
        sector = random.choice(sectors)
        rows.append({
            "Account Number": f"CDP-ANON-{i:04d}-{random.choice([2022, 2023])}",
            "Organization": f"European Company {i+1}",
            "Primary Sector": sector,
            "Primary Industry": sector,
            "Country": country,
            "ISIN": None,  # häufig fehlend in echten CDP-Daten
            "Reporting Year": random.choice([2022, 2023]),
            "Scope 1 (metric tons CO2e)": round(random.uniform(10_000, 5_000_000)) if random.random() > 0.1 else None,
            "Scope 2 Location-Based (metric tons CO2e)": round(random.uniform(5_000, 2_000_000)) if random.random() > 0.2 else None,
            "Scope 2 Market-Based (metric tons CO2e)": None,
            "Scope 3 Total (metric tons CO2e)": None,
            "Emission Unit": random.choice(["metric tons CO2e", "tonnes CO2", ""]),
            "Data Verification": random.choice(["Third-party verified", "Not verified", ""]),
            "CDP Score": random.choice(["A", "B", "C", "D", None, None]),
            "Public Disclosure": random.choice(["Yes", "No"]),
        })

    return pd.DataFrame(rows)


def fetch_cdp() -> None:
    """
    Versucht den CDP Open Data Socrata-Endpoint.
    Fallback auf realistisch generierte CSV-Daten mit echten Datenqualitätsproblemen.
    """
    print("\n[2/4] CDP Open Data ->", OUTPUT_DIR / "cdp_emissions.csv")
    out = OUTPUT_DIR / "cdp_emissions.csv"

    # Bekannte Socrata Dataset-IDs für CDP Climate Data (kann sich ändern)
    CDP_URLS = [
        "https://data.cdp.net/resource/qexj-wejd.csv?$limit=2000",
        "https://data.cdp.net/resource/3xc2-u7wa.csv?$limit=2000",
    ]
    for url in CDP_URLS:
        try:
            log(f"Versuche: {url}")
            resp = requests.get(url, timeout=TIMEOUT, headers={"Accept": "text/csv"})
            if resp.status_code == 200 and resp.text.startswith(("account", "Account", "organiz", "Organiz")):
                df = pd.read_csv(pd.io.common.StringIO(resp.text))
                # Auf europäische Länder filtern falls Spalte vorhanden
                eu_countries = {"Germany", "France", "United Kingdom", "Netherlands",
                                "Switzerland", "Sweden", "Spain", "Italy", "Ireland",
                                "Denmark", "Norway", "Finland", "Belgium", "Austria"}
                country_col = next((c for c in df.columns if "country" in c.lower()), None)
                if country_col:
                    df = df[df[country_col].isin(eu_countries)]
                df.to_csv(out, index=False, encoding="utf-8")
                log(f"✓ CDP-Daten geladen ({len(df)} Zeilen, {len(df.columns)} Spalten)")
                record_file(out)
                return
        except requests.RequestException as e:
            log(f"  Endpoint nicht erreichbar: {e}")

    log("⚠ CDP Socrata-Endpoint nicht erreichbar — verwende Fallback-Daten")
    df = _cdp_fallback_df()
    df.to_csv(out, index=False, encoding="utf-8")
    log(f"✓ Fallback gespeichert ({len(df)} Zeilen, echte Datenqualitätsprobleme inkl. Nullwerte)")
    record_file(out)


# ---------------------------------------------------------------------------
# Quelle 3: Our World in Data (OWID)
# ---------------------------------------------------------------------------

def fetch_owid() -> None:
    """
    Lädt OWID CO2-Länderdaten von GitHub, filtert auf europäische Länder
    und die letzten 5 Jahre.
    """
    print("\n[3/4] Our World in Data ->", OUTPUT_DIR / "owid_co2_countries.csv")
    out = OUTPUT_DIR / "owid_co2_countries.csv"
    url = "https://raw.githubusercontent.com/owid/co2-data/master/owid-co2-data.csv"

    eu_countries = {
        "Germany", "France", "United Kingdom", "Italy", "Spain", "Netherlands",
        "Belgium", "Switzerland", "Sweden", "Norway", "Denmark", "Finland",
        "Austria", "Poland", "Portugal", "Ireland", "Czechia", "Romania",
        "Hungary", "Greece",
    }
    cutoff_year = date.today().year - 6

    try:
        log(f"Download: {url}")
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        df = pd.read_csv(pd.io.common.StringIO(resp.text))
        df_eu = df[df["country"].isin(eu_countries) & (df["year"] >= cutoff_year)]
        df_eu.to_csv(out, index=False, encoding="utf-8")
        log(f"✓ OWID-Daten geladen und gefiltert ({len(df_eu)} Zeilen, "
            f"{df_eu['country'].nunique()} Länder, {df_eu['year'].min()}–{df_eu['year'].max()})")
        record_file(out)
    except requests.RequestException as e:
        log(f"✗ OWID nicht erreichbar: {e}")
        log("  Überprüfe Internetverbindung und versuche erneut.")


# ---------------------------------------------------------------------------
# Quelle 4: Synthetische Fondsdaten
# ---------------------------------------------------------------------------

def generate_fund_data() -> None:
    """
    Erstellt zwei CSV-Dateien mit synthetischen Fondsdaten.
    fund_master.csv:    10 Deka-ähnliche Fonds
    fund_positions.csv: Gewichtungen mit echten ISINs aus den anderen Quellen
    """
    print("\n[4/4] Synthetische Fondsdaten ->", OUTPUT_DIR / "fund_master.csv",
          "und", OUTPUT_DIR / "fund_positions.csv")
    random.seed(21)

    # --- fund_master.csv ---
    fonds = [
        ("DE000DK0EC05", "DK0EC0", "Deka-Nachhaltigkeit Aktien CF",         "Aktienfonds",         "EUR", "2001-03-15", "Luxembourg"),
        ("DE000DK0EQ06", "DK0EQ0", "Deka-Nachhaltigkeit Renten CF",         "Rentenfonds",         "EUR", "2003-07-01", "Luxembourg"),
        ("DE0008474503", "847450", "DekaFonds CF",                           "Aktienfonds",         "EUR", "1956-01-01", "Germany"),
        ("DE000DK2CDF5", "DK2CDF", "Deka-Europa Aktien Spezial CF",          "Aktienfonds",         "EUR", "2015-06-30", "Luxembourg"),
        ("DE000DK0EMF2", "DK0EMF", "Deka-GlobalChampions CF",                "Aktienfonds",         "EUR", "2000-09-01", "Luxembourg"),
        ("DE000DK0EYJ4", "DK0EYJ", "Deka-Klimawandel & Biodiversitaet CF",  "Thematischer Fonds",  "EUR", "2021-04-19", "Luxembourg"),
        ("DE000DK0ESD0", "DK0ESD", "Deka-ESG MSCI World Climate Paris CF",   "Indexfonds",          "EUR", "2022-01-17", "Luxembourg"),
        ("DE000DK0ETR2", "DK0ETR", "Deka-EuropaSelect CF",                   "Aktienfonds",         "EUR", "1999-10-04", "Germany"),
        ("DE000DK0EMX6", "DK0EMX", "Deka-Wandelanleihen CF",                 "Mischfonds",          "EUR", "2008-03-03", "Luxembourg"),
        ("DE000DK0EBR7", "DK0EBR", "Deka-Basisstrategie Aktien CF",          "Aktienfonds",         "EUR", "2018-09-17", "Luxembourg"),
    ]

    df_master = pd.DataFrame(fonds, columns=[
        "fund_isin", "fund_wkn", "fund_name", "fund_type",
        "currency", "inception_date", "domicile"
    ])
    out_master = OUTPUT_DIR / "fund_master.csv"
    df_master.to_csv(out_master, index=False, encoding="utf-8")
    log(f"✓ fund_master.csv ({len(df_master)} Fonds)")
    record_file(out_master)

    # --- fund_positions.csv ---
    all_isins = [co["isin"] for co in EU_COMPANIES]
    position_rows = []

    for _, fund in df_master.iterrows():
        for ref_date in ["2023-12-31", "2024-06-30"]:
            # Jeder Fonds hält 12–20 Positionen
            n = random.randint(12, 20)
            holdings = random.sample(all_isins, n)

            # Gewichtungen: Dirichlet-ähnlich, summiert auf ~100%
            raw_weights = [random.uniform(1, 15) for _ in range(n)]
            total = sum(raw_weights)
            weights = [round(w / total * 100, 4) for w in raw_weights]
            # Letzte Position anpassen damit Summe exakt 100 ergibt
            weights[-1] = round(100 - sum(weights[:-1]), 4)

            for isin, weight in zip(holdings, weights):
                position_rows.append({
                    "fund_isin": fund["fund_isin"],
                    "holding_isin": isin,
                    "weight_pct": weight,
                    "position_date": ref_date,
                })

    df_positions = pd.DataFrame(position_rows)
    out_positions = OUTPUT_DIR / "fund_positions.csv"
    df_positions.to_csv(out_positions, index=False, encoding="utf-8")
    log(f"✓ fund_positions.csv ({len(df_positions)} Zeilen, "
        f"{df_positions['fund_isin'].nunique()} Fonds × 2 Stichtage)")
    record_file(out_positions)


# ---------------------------------------------------------------------------
# Hauptprogramm
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("  ESG Beispieldaten Generator")
    print("=" * 60)
    print(f"  Ausgabe: {OUTPUT_DIR.resolve()}")

    fetch_nzdpu()
    fetch_cdp()
    fetch_owid()
    generate_fund_data()

    print("\n" + "=" * 60)
    print("  Ergebnis")
    print("=" * 60)
    if CREATED_FILES:
        df_summary = pd.DataFrame(CREATED_FILES)
        print(df_summary.to_string(index=False))
    else:
        print("  Keine Dateien erstellt — alle Quellen fehlgeschlagen.")
    print()
