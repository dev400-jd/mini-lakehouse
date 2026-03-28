# Setup-Anleitung

## Voraussetzungen

### Docker Desktop

Docker Desktop 4.x oder hoeher.

**Windows (WSL2):**

1. Docker Desktop installieren: https://www.docker.com/products/docker-desktop/
2. WSL2-Backend aktivieren: Docker Desktop → Settings → General → "Use the WSL 2 based engine"
3. RAM fuer WSL2 konfigurieren — standardmaessig nutzt WSL2 nur 50 % des RAM.
   Datei `%USERPROFILE%\.wslconfig` anlegen oder anpassen:

```ini
[wsl2]
memory=12GB
processors=4
```

4. WSL2 neu starten:

```powershell
wsl --shutdown
```

**macOS / Linux:**

Docker Desktop installieren und unter Settings → Resources mindestens 12 GB RAM zuweisen.

### Weitere Tools

- **git** — zum Klonen des Repositories
- **make** — fuer die Makefile-Kommandos (Windows: via Git Bash, WSL oder Chocolatey)
- **uv** — Python-Paketmanager, nur fuer die optionale Datengenerierung

```bash
# uv installieren (Linux/macOS/WSL)
curl -LsSf https://astral.sh/uv/install.sh | sh

# uv installieren (Windows PowerShell)
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

---

## Installation

```bash
# 1. Repository klonen
git clone https://github.com/dev400-jd/mini-lakehouse.git
cd mini-lakehouse

# 2. Services starten
docker compose up -d

# Warten bis alle Services healthy sind (ca. 60-90 Sekunden):
docker compose ps

# 3. Beispieldaten laden
make seed
```

`make seed` fuehrt aus:
- `scripts/generate-nzdpu-v2.py` — erzeugt V2-Testdaten
- `scripts/spark-ingestion.py` via spark-submit — laedt alle 5 Tabellen in den Raw Layer
- Prueft Zeilenanzahl in Trino

---

## Services & URLs

| Service | URL | Zugangsdaten |
|---------|-----|--------------|
| Jupyter | http://localhost:8888?token=lakehouse | Token: `lakehouse` |
| MinIO Console | http://localhost:9001 | `lakehouse` / `lakehouse123` |
| Nessie UI | http://localhost:19120 | keins |
| Trino Web UI | http://localhost:8080 | keins |
| Spark Master UI | http://localhost:8081 | keins |
| PostgreSQL | localhost:5432 | `lakehouse` / `lakehouse123` |

---

## Konfiguration

Alle Einstellungen stehen in `.env`. Die Datei ist nicht im Repository (`.gitignore`).
Beim ersten Klonen liegt `.env.example` als Vorlage bereit:

```bash
cp .env.example .env
# Anpassen falls Ports belegt sind
```

---

## Notebooks ausfuehren

Notebook 01 kann beliebig oft ausgefuehrt werden (nur lesend).

Notebook 02 veraendert die Tabelle `nzdpu_emissions` (Schema Evolution + Append).
Vor jedem erneuten Durchlauf von Notebook 02 muss der Ausgangszustand wiederhergestellt werden:

```bash
make seed
```

---

## Bekannte Probleme

### Port-Konflikte

Wenn ein Port bereits belegt ist, schlaegt `docker compose up` fehl.
Ports in `.env` aendern:

```env
TRINO_PORT=18080        # statt 8080
JUPYTER_PORT=18888      # statt 8888
```

### Docker Memory — Spark startet nicht

Symptom: `spark-master` oder `spark-worker` startet und faellt sofort wieder aus.
Loesung: Docker Desktop → Settings → Resources → Memory auf mindestens 8 GB setzen.

### ARM / Apple Silicon

Alle verwendeten Images unterstuetzen `linux/amd64` und `linux/arm64`.
Falls ein Image nicht startet: `docker compose pull` ausfuehren um die aktuellsten Manifests zu laden.

### make nicht gefunden (Windows)

Git Bash enthaelt `make` nicht standardmaessig. Optionen:
- Via Chocolatey: `choco install make`
- Via Scoop: `scoop install make`
- Oder: Kommandos aus dem Makefile manuell ausfuehren
