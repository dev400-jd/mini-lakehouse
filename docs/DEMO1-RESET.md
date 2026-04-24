# Demo 1 — Reset-Script

> **WARNUNG:** Dieses Script droppt Tabellen ohne Backup und regeneriert
> synthetische Demo-Daten. Nur für Demo-Zwecke — niemals in Produktion.

## Zweck

`scripts/reset-demo1.sh` stellt in ca. 1–2 Minuten einen konsistenten
Startzustand für Demo 1 her. Geeignet für Generalprobe, Livebetrieb
und Wiederholung nach Problemen.

## Was passiert (5 Schritte)

| Schritt | Aktion |
|---------|--------|
| 0 | Voraussetzungs-Check: Stack läuft, Trino/Spark/Jupyter erreichbar |
| 1 | Tabellen droppen: `snp_fondspreise_scd2` → `stg_fondspreise` → `raw.fondspreise` (umgekehrte Dependency-Reihenfolge), Schemas sicherstellen |
| 2 | Demo-Daten neu generieren via `generate-fondspreise.py` (Seed 20260422, deterministisch) |
| 3 | Load 1 ingestieren → `nessie.raw.fondspreise` (1 Row) |
| 4 | Staging aufbauen → `nessie.staging.stg_fondspreise` (450 Zeilen) |
| 5 | Snapshot Erstrun → `nessie.curated.snp_fondspreise_scd2` (450 Zeilen, alle `dbt_valid_to = NULL`) |

## Was NICHT passiert

- **Load 2** wird nicht ausgeführt — das ist der Aha-Moment der Live-Demo
- **ESG-Tabellen** (`raw.nzdpu_emissions`, `raw.cdp_emissions`, `staging.stg_*_emissions`,
  `curated.curated_esg_emissions`) bleiben unberührt

## Aufruf

**Bash direkt (vom Repo-Root):**
```bash
bash scripts/reset-demo1.sh
```

**Make:**
```bash
make reset-demo1
```

**Aus PowerShell via WSL2:**
```powershell
wsl bash scripts/reset-demo1.sh
```

## Erwartete Laufzeit

| Schritt | Zeit |
|---------|------|
| Drops + Schemas | 5–10s |
| Datengenerierung | 2–5s |
| Spark-Ingest | 20–40s |
| dbt run staging | 15–30s |
| dbt snapshot | 15–30s |
| **Gesamt** | **~1–2 Minuten** |

Wenn das Script länger als 3 Minuten läuft, liegt ein Problem vor.

## Troubleshooting

**"Trino nicht erreichbar"**
```bash
docker compose up -d
docker compose ps  # alle Services healthy abwarten
```

**"spark-master nicht erreichbar"**
```bash
docker compose up -d spark-master spark-worker
```

**dbt schlägt fehl ("table not found" o.ä.)**  
Staging- oder Curated-Schema fehlt. Das Script legt die Schemas
idempotent an — beim nächsten Lauf sollte es funktionieren. Falls
nicht, manuell in CloudBeaver prüfen:
```sql
SHOW SCHEMAS IN nessie;
```

**Script bricht bei Schritt 3 ab (Spark-Ingest)**  
Spark-Startup-Timeout. Erneut ausführen — nach einem Warmlauf
starten die Container schneller.

**Idempotenz prüfen (2x laufen lassen):**
```bash
bash scripts/reset-demo1.sh && bash scripts/reset-demo1.sh
```
Beide Läufe müssen mit demselben Endzustand abschließen.

## Verifikation nach Reset

```powershell
docker compose exec trino trino --execute "SELECT COUNT(*) FROM nessie.raw.fondspreise"
docker compose exec trino trino --execute "SELECT COUNT(*) FROM nessie.staging.stg_fondspreise"
docker compose exec trino trino --execute "SELECT COUNT(*) FROM nessie.curated.snp_fondspreise_scd2"
docker compose exec trino trino --execute "SELECT COUNT(*) FROM nessie.curated.snp_fondspreise_scd2 WHERE dbt_valid_to IS NULL"
```

Erwartete Werte: `1`, `450`, `450`, `450`.
