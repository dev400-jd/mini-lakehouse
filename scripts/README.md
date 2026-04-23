# scripts/

Hilfsskripte für das Mini-Lakehouse-Projekt.

## generate-fondspreise.py

Erzeugt zwei synthetische Fondspreis-Datasets für Demo 1 (Iceberg Time Travel / SCD2).

```bash
uv run python scripts/generate-fondspreise.py
```

**Ausgabe:**

| Datei | Inhalt |
|-------|--------|
| `data/sample/fondspreise_load1.json` | 450 Records (5 Fonds × 90 Handelstage, 2026-01-05 bis 2026-05-08) |
| `data/sample/fondspreise_load2_correction.json` | 1 Record (NAV-Korrektur für DE000A1JX0V2 am 2026-04-20) |

**Hinweise:**
- Seed `20260422` — zweimalige Ausführung erzeugt identische Dateien.
- 2026-04-19 ist Sonntag; Korrekturdatum ist der nächste Werktag 2026-04-20 (Montag).
- ISIN `DE000A1JX0V2` hat eine gültige Mod-10/Luhn-Prüfziffer.

---

## ingest-fondspreise.py

Ingestiert eine Fondspreis-JSON-Datei in die Iceberg-Tabelle `nessie.raw.fondspreise`.
Erstellt die Tabelle beim ersten Aufruf automatisch (idempotent).

```bash
docker compose exec spark-master \
  spark-submit /scripts/ingest-fondspreise.py \
    --file /data/sample/fondspreise_load1.json \
    --ingestion-timestamp 2026-04-20T08:15:00Z
```

| Option | Beschreibung |
|--------|-------------|
| `--file` | Pfad zur JSON-Datei (innerhalb des Containers: `/data/sample/...`) |
| `--ingestion-timestamp` | UTC-Timestamp (ISO 8601). Optional — Default: aktuelle Zeit. |

Detaillierte Ausführungsanleitung: [docs/DEMO1-INGESTION.md](../docs/DEMO1-INGESTION.md)

---

## verify-fondspreise-ingestion.py

Prüft `nessie.raw.fondspreise` nach beiden Ingestion-Läufen auf Korrektheit.

```bash
docker compose exec spark-master \
  spark-submit /scripts/verify-fondspreise-ingestion.py
```

Prüft: Gesamt-Records (451), DISTINCT ingestion_id (2), Iceberg-Snapshots (2),
raw_payload JSON-roundtrip.
