#!/usr/bin/env python3
"""Generate synthetic fund price datasets for Demo 1 (Iceberg Time Travel / SCD2)."""

import hashlib
import json
import random
from datetime import date, timedelta
from pathlib import Path

SEED = 20260422
MAIN_ISIN = "DE000A1JX0V2"
MAIN_FUND_NAME = "TestFonds Aktien Europa"

# 2026-04-19 is Sunday → nearest weekday is Monday 2026-04-20
CORRECTION_DATE = date(2026, 4, 20)
CORRECTION_DELTA = -0.68

OUTPUT_DIR = Path("data/sample")


def _isin_to_digits(isin: str) -> str:
    return "".join(str(ord(c) - ord("A") + 10) if c.isalpha() else c for c in isin)


def _luhn_check_digit(partial_isin: str) -> int:
    for check in range(10):
        digits = [int(d) for d in _isin_to_digits(partial_isin + str(check))][::-1]
        total = 0
        for i, d in enumerate(digits):
            if i % 2 == 1:
                d = d * 2 - 9 if d * 2 > 9 else d * 2
            total += d
        if total % 10 == 0:
            return check
    raise ValueError(f"No valid Luhn check digit found for {partial_isin}")


def _trading_days(start: date, n: int) -> list[date]:
    days, cur = [], start
    while len(days) < n:
        if cur.weekday() < 5:
            days.append(cur)
        cur += timedelta(days=1)
    return days


def _nav_series(start_nav: float, n: int, rng: random.Random) -> list[float]:
    navs = [start_nav]
    for _ in range(n - 1):
        change = max(-0.04, min(0.04, rng.gauss(0, 0.015)))
        navs.append(round(navs[-1] * (1 + change), 2))
    return navs


def main() -> None:
    rng = random.Random(SEED)

    OTHER_FUNDS = [
        ("TestFonds Renten Global", "EUR"),
        ("TestFonds EM Equity",     "EUR"),
        ("TestFonds Balanced",      "EUR"),
        ("TestFonds US Growth",     "USD"),
    ]

    # Main fund first, then 4 synthetic — order determines RNG sequence
    funds = [
        {
            "isin": MAIN_ISIN,
            "fund_name": MAIN_FUND_NAME,
            "currency": "EUR",
            "start_nav": round(rng.uniform(80, 200), 2),
        }
    ]
    for name, currency in OTHER_FUNDS:
        body = "DE" + "".join(str(rng.randint(0, 9)) for _ in range(9))
        isin = body + str(_luhn_check_digit(body))
        funds.append(
            {
                "isin": isin,
                "fund_name": name,
                "currency": currency,
                "start_nav": round(rng.uniform(80, 200), 2),
            }
        )

    days = _trading_days(date(2026, 1, 5), 90)
    assert len(days) == 90
    assert days[0] == date(2026, 1, 5), "Start must be 2026-01-05"
    assert days[-1] >= date(2026, 5, 8), f"Last day {days[-1]} before 2026-05-08"
    assert CORRECTION_DATE in days, f"{CORRECTION_DATE} not in trading days"

    corr_idx = days.index(CORRECTION_DATE)
    main_load1_nav: float | None = None
    records: list[dict] = []

    for fund in funds:
        navs = _nav_series(fund["start_nav"], 90, rng)
        if fund["isin"] == MAIN_ISIN:
            main_load1_nav = navs[corr_idx]
        for i, day in enumerate(days):
            records.append(
                {
                    "isin": fund["isin"],
                    "fund_name": fund["fund_name"],
                    "business_date": day.isoformat(),
                    "nav": navs[i],
                    "currency": fund["currency"],
                }
            )

    records.sort(key=lambda r: (r["business_date"], r["isin"]))
    assert len(records) == 450, f"Expected 450 records, got {len(records)}"
    assert main_load1_nav is not None

    corrected_nav = round(main_load1_nav + CORRECTION_DELTA, 2)
    assert corrected_nav != main_load1_nav, "Correction must differ from Load-1 value"

    load1 = {
        "source_system": "fondsdaten_provider_xyz",
        "source_version": "v1",
        "generated_at": "2026-04-22T06:00:00Z",
        "record_count": len(records),
        "records": records,
    }

    load2 = {
        "source_system": "fondsdaten_provider_xyz",
        "source_version": "v1-correction",
        "generated_at": "2026-04-22T14:37:00Z",
        "correction_reason": "NAV-Fehler im initialen Load — korrigierte Werte nachgeliefert",
        "record_count": 1,
        "records": [
            {
                "isin": MAIN_ISIN,
                "fund_name": MAIN_FUND_NAME,
                "business_date": CORRECTION_DATE.isoformat(),
                "nav": corrected_nav,
                "currency": "EUR",
            }
        ],
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path1 = OUTPUT_DIR / "fondspreise_load1.json"
    path2 = OUTPUT_DIR / "fondspreise_load2_correction.json"

    path1.write_text(json.dumps(load1, indent=2, ensure_ascii=False), encoding="utf-8")
    path2.write_text(json.dumps(load2, indent=2, ensure_ascii=False), encoding="utf-8")

    h1 = hashlib.sha256(path1.read_bytes()).hexdigest()
    h2 = hashlib.sha256(path2.read_bytes()).hexdigest()

    print(f"  Load 1: {len(records)} records -> {path1}")
    print(f"  Load 2: 1 record -> {path2}")
    print()
    print("  Correction check:")
    print(f"    ISIN {MAIN_ISIN}, {CORRECTION_DATE.isoformat()}:")
    print(f"      Load 1 NAV: {main_load1_nav:.2f} EUR")
    print(f"      Load 2 NAV: {corrected_nav:.2f} EUR (correction)")
    print()
    print("  File hashes:")
    print(f"    {path1.name}: sha256:{h1}")
    print(f"    {path2.name}: sha256:{h2}")


if __name__ == "__main__":
    main()
