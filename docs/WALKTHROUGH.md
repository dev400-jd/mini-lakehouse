# Walkthrough — Hackathon-Auftakt

Kuratierte Obermenge der bestehenden Befehlssammlungen für den Workshop-Tag.
Teil A ist das Drehbuch für die Live-Demo (60–90 min) entlang des Datenflusses,
Teil B der Spickzettel für die Gruppenarbeit.

Alle Werte stammen aus `.env`, `docker-compose.yml`, `Makefile`, `config/**` und
den bestehenden Docs. Bestehende Detaildokumente werden verlinkt, nicht kopiert:

| Thema | Quelle |
|-------|--------|
| Installation, WSL2, Port-Konflikte | [SETUP.md](SETUP.md) |
| Komponenten, Layer-Architektur | [ARCHITECTURE.md](ARCHITECTURE.md) |
| Vollständige dbt-Befehlsreferenz | [DBT-COMMANDS.md](DBT-COMMANDS.md) |
| Quality Gate curated→trusted | [DEMO2-QUALITY-GATE.md](DEMO2-QUALITY-GATE.md) |
| Demo-Drehbücher (Fondspreise / ESG) | [DEMO1-DREHBUCH.md](DEMO1-DREHBUCH.md), [DEMO2-DREHBUCH.md](DEMO2-DREHBUCH.md) |
| Reset / State-Machine | [DEMO1-RESET.md](DEMO1-RESET.md), [DEMO2-STATES.md](DEMO2-STATES.md) |

Arbeitsverzeichnis für alle Befehlsblöcke ist das Repo-Root, sofern nicht anders
angegeben. Shell ist Git Bash oder PowerShell; abweichende Formen sind markiert.

---

# Teil A — Rundgang

## Station 0: Boot & Kontrolle

**Kernbotschaft:** Der gesamte Stack ist ein `docker compose up -d` — acht
Container, deren Gesundheit man an einer einzigen Spalte abliest.

```bash
docker compose up -d
docker compose ps
```

Gesund heißt: `minio`, `postgres`, `nessie`, `trino`, `spark-master`, `jupyter`,
`cloudbeaver` stehen auf `Up (healthy)`. `spark-worker` läuft ohne Healthcheck
(zeigt nur `Up`) — das ist kein Fehler. Die beiden Init-Container `minio-init`
und `trino-init` laufen einmalig durch und beenden sich mit Exit 0; dass sie in
`docker compose ps` nicht mehr auftauchen, ist der Normalzustand.

Startdauer bis alle Healthchecks grün sind: ca. 60–90 Sekunden ([SETUP.md](SETUP.md)).

```bash
docker stats --no-stream
docker compose logs -f trino
docker compose logs -f spark-master spark-worker
```

Beispieldaten laden (ca. 2–3 Minuten, idempotent):

```bash
make seed
# ohne make (Windows):
bash scripts/seed-data.sh
```

`make seed` ruft `scripts/seed-data.sh` auf: prüft die Health von `minio`,
`postgres`, `nessie`, `spark-master`, lädt die Raw-Tabellen (geparste Referenz­tabellen
per `scripts/spark-ingestion.py`, File-level cdp/nzdpu per `init-*`/`ingest-*`-Skripten),
legt die Schemas `staging` und `curated` an und verifiziert die Zeilenzahlen über Trino.
Erwartetes Ergebnis — fünf Tabellen in `nessie.raw`:

| Tabelle | Zeilen | Form |
|---------|--------|------|
| `nzdpu_emissions` | 1 | File-level (`raw_payload`) — dbt-Staging entpackt |
| `cdp_emissions` | 1 | File-level (`raw_payload`) — dbt-Staging entpackt |
| `owid_co2_countries` | 100 | geparst |
| `fund_master` | 10 | geparst |
| `fund_positions` | 319 | geparst |

Das Skript endet mit `Raw Layer bereit.` — das ist das Signal, dass der Stack
demofähig ist.

> `make health` ist im Makefile definiert, ruft aber `scripts/healthcheck.sh`
> auf — diese Datei existiert im Repo nicht. Das Target schlägt fehl; für den
> Gesundheitscheck `docker compose ps` verwenden.

---

## Station 1: Storage — MinIO

**Kernbotschaft:** Eine Iceberg-Tabelle ist kein Dateiformat, sondern ein
Verzeichnis aus Daten und Metadaten auf gewöhnlichem Objektspeicher.

MinIO Console: <http://localhost:9001> — Login `lakehouse` / `lakehouse123`.

Fünf Buckets bilden die Zonen der Pipeline ab (angelegt von `minio-init` aus
`scripts/init-buckets.sh`):

| Bucket | Zone | Beschrieben von |
|--------|------|-----------------|
| `raw` | Daten wie geliefert, append-only | Spark |
| `staging` | bereinigt, typisiert | dbt via Trino |
| `curated` | fachlich aggregiert | dbt via Trino |
| `trusted` | nach Quality Gate freigegeben | dbt via Trino |
| `warehouse` | Default-Warehouse des Katalogs | Spark/Trino |

**Navigation in der Console:** Bucket `raw` → `fund_positions/` → zwei Ordner:

```
raw/fund_positions/
├── data/
│   ├── position_date=2023-12-31/00000-30-....parquet
│   └── position_date=2024-06-30/00000-30-....parquet
└── metadata/
    ├── 00000-....metadata.json     Schema, Partitionen, Snapshot-Liste
    ├── snap-3931422776536639655-1-....avro   Manifest List
    └── ....-m0.avro                Manifest File mit Min/Max-Statistiken
```

Der Ordnername `position_date=2023-12-31` ist die Partitionierung — sichtbar im
Pfad, aber im SQL nie erwähnt. Die Parquet-Dateien sind direkt herunterladbar:
kein Lock-in, jede Engine kann sie lesen.

Dieselbe Struktur auf der Kommandozeile:

```bash
docker run --rm --network mini-lakehouse_lakehouse-net --entrypoint sh minio/mc -c \
  "mc alias set lh http://minio:9000 lakehouse lakehouse123 && \
   mc ls lh/ && \
   mc ls --recursive lh/raw/fund_positions/"
```

**Neuen Bucket anlegen** (Andockpunkt für die Gruppenprojekte — z. B. eigene
Rohdaten oder ein Artefakt-Store):

Console: Buckets → *Create Bucket* → Name eingeben → *Create Bucket*.

Oder per CLI:

```bash
docker run --rm --network mini-lakehouse_lakehouse-net --entrypoint sh minio/mc -c \
  "mc alias set lh http://minio:9000 lakehouse lakehouse123 && \
   mc mb --ignore-existing lh/<bucket-name> && \
   mc ls lh/"
```

Damit ein neuer Bucket als Iceberg-Layer nutzbar wird, braucht er ein Schema mit
passender Location — Muster aus `scripts/init-schemas.sh`:

```sql
CREATE SCHEMA IF NOT EXISTS nessie.<name> WITH (location = 's3a://<bucket-name>/');
```

---

## Station 2: Katalog — Nessie

**Kernbotschaft:** Nessie ist der Katalog, nicht der Speicher — er weiß, welche
Tabellen existieren und in welcher Version, die Daten selbst liegen auf MinIO.

Nessie UI: <http://localhost:19120> — keine Zugangsdaten.

Konfiguration prüfen (derselbe Endpoint, den der Healthcheck nutzt):

```bash
curl -s http://localhost:19120/api/v2/config
```

```json
{
  "defaultBranch" : "main",
  "minSupportedApiVersion" : 1,
  "maxSupportedApiVersion" : 2,
  "actualApiVersion" : 2,
  "specVersion" : "2.2.0"
}
```

**Namespaces (= Layer) über die REST-API listen:**

```bash
curl -s "http://localhost:19120/api/v2/trees/main/entries?filter=entry.contentType%3D%3D%27NAMESPACE%27"
```

Liefert die vier Layer-Namespaces `raw`, `staging`, `curated`, `trusted` — exakt
die Schemas, die Trino unter `SHOW SCHEMAS IN nessie` zeigt, und die Buckets aus
Station 1. Alle Referenzen (Branches) listen:

```bash
curl -s http://localhost:19120/api/v2/trees
```

Alle Einträge inkl. Tabellen auf dem Branch `main`:

```bash
curl -s "http://localhost:19120/api/v2/trees/main/entries"
```

**Wichtig für die Gruppen:** Spark und Trino sprechen unterschiedliche
API-Versionen desselben Katalogs an — das ist Absicht und keine Fehlkonfiguration:

| Engine | Nessie-URI | Quelle |
|--------|------------|--------|
| Spark (Cluster + Notebooks) | `http://nessie:19120/api/v2` | `config/spark/spark-defaults.conf`, `notebooks/spark_init.py` |
| Trino | `http://nessie:19120/api/v1` | `config/trino/catalog/nessie.properties` |

Branch in beiden Fällen: `main`. Warehouse: `s3a://warehouse/` (Spark) bzw.
`s3://warehouse/` (Trino).

---

## Station 3: Compute — Spark in JupyterLab

**Kernbotschaft:** Ein paar Zeilen Konfiguration genügen, damit Spark den
Katalog kennt — danach ist eine Iceberg-Tabelle einfach eine Tabelle.

JupyterLab: <http://localhost:8888?token=lakehouse>

Das fertige Notebook unter `notebooks/` ist der schnellste Einstieg:

| Notebook | Inhalt |
|----------|--------|
| `01_iceberg_erkunden.ipynb` | Anatomie einer Iceberg-Tabelle: Data Files, Manifests, Snapshots, Partitionen |

Es setzt `make seed` voraus und liest nur (verändert keine Tabellen). Die Time-Travel-
und Schema-Evolution-Story wird in Station 4 per Trino auf `nessie.raw.fondspreise`
gezeigt (zwei Snapshots nach Demo-1-Load-2).

**Spark-Session gegen den Katalog** — erste Zelle in jedem Notebook. Die
Konfiguration steckt in `notebooks/spark_init.py`, nicht im Notebook:

```python
import sys
sys.path.insert(0, "/home/jovyan/notebooks")
from spark_init import get_spark_session, trino_query, show

spark = get_spark_session("hackathon-scratch")
```

`get_spark_session()` läuft als `local[*]` direkt im Jupyter-Container — es wird
kein Spark-Cluster benötigt. Der Katalog heißt `nessie`, zeigt auf
`http://nessie:19120/api/v2`, Branch `main`, Warehouse `s3a://warehouse/`.

**Lesen:**

```python
spark.sql("SHOW TABLES IN nessie.raw").show()
spark.sql("SELECT * FROM nessie.raw.fund_positions LIMIT 10").show()

# Metadaten-Tabellen (Spark-Syntax: Punkt-Notation)
spark.sql("SELECT file_path, record_count FROM nessie.raw.fund_positions.files").show(truncate=False)
spark.sql("SELECT snapshot_id, committed_at, operation FROM nessie.raw.fund_positions.snapshots").show()
```

**Schreiben** — Minimalbeispiel, erzeugt eine neue Iceberg-Tabelle im
Staging-Layer und einen zweiten Snapshot per Append:

```python
df = spark.createDataFrame(
    [("DE0007236101", 2024, 1200.5), ("DE0005557508", 2024, 890.0)],
    ["isin", "reporting_year", "value"],
)

# CREATE + erster Snapshot
df.writeTo("nessie.staging.demo_scratch").createOrReplace()

# zweiter Snapshot
df.writeTo("nessie.staging.demo_scratch").append()

spark.sql("SELECT snapshot_id, operation, summary['added-records'] AS added "
          "FROM nessie.staging.demo_scratch.snapshots ORDER BY committed_at").show()
```

Die Tabelle ist unmittelbar in Trino sichtbar (`nessie.staging.demo_scratch`) —
dieselben Dateien, anderer Motor. Aufräumen:

```python
spark.sql("DROP TABLE IF EXISTS nessie.staging.demo_scratch")
```

Aus dem Notebook heraus lässt sich auch Trino abfragen — `trino_query()` liefert
einen pandas-DataFrame:

```python
trino_query("SELECT count(*) FROM nessie.raw.fund_positions")
```

Der Spark-Cluster (Master-UI <http://localhost:8081>, Master-Port 7077) wird von
den Notebooks nicht genutzt, sondern von `make seed` per `spark-submit`:

```bash
docker compose exec -T spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 /scripts/spark-ingestion.py
```

Unter Git Bash `MSYS_NO_PATHCONV=1` voranstellen (siehe Station 7).

---

## Station 4: Query — Trino & CloudBeaver

**Kernbotschaft:** Trino hat nie von Spark gehört — es liest dieselben Dateien
über denselben Katalog, inklusive Versionshistorie.

| Zugang | URL | Zugangsdaten |
|--------|-----|--------------|
| Trino Web UI | <http://localhost:8080> | beliebiger Username, kein Passwort |
| CloudBeaver | <http://localhost:8978> | Admin-Passwort beim Erststart selbst setzen |
| Trino CLI | `docker compose exec trino trino` | — |

Die Trino Web UI zeigt laufende und abgeschlossene Queries (Query-Details,
Stages, Splits) — nützlich, um zu zeigen, dass CloudBeaver, dbt und die
Notebooks alle auf derselben Engine landen.

In CloudBeaver ist die Verbindung **„Lakehouse (Trino)"** vorkonfiguriert
(`config/cloudbeaver/initial-data-sources.conf`): `jdbc:trino://trino:8080/nessie`,
User `trino`, kein Passwort. SQL-Editor öffnen und der Reihe nach:

**1. Layer zeigen**

```sql
SHOW SCHEMAS IN nessie;
```

Liefert `raw`, `staging`, `curated`, `trusted` plus `information_schema` — die
Namespaces aus Station 2.

```sql
SHOW TABLES IN nessie.trusted;
```

**2. Query auf eine Trusted-Tabelle**

```sql
SELECT source_system,
       count(*)                        AS rows,
       round(avg(scope_1_tco2e))       AS avg_scope1
FROM nessie.trusted.trusted_esg_emissions
GROUP BY source_system
ORDER BY rows DESC;
```

`nessie.trusted.trusted_esg_emissions` enthält nach grünem Quality Gate 150 Zeilen.

**3. Snapshots — die Versionshistorie**

```sql
SELECT snapshot_id,
       committed_at,
       operation,
       summary['added-records'] AS added_records
FROM nessie.raw."fund_positions$snapshots"
ORDER BY committed_at;
```

Trino-Syntax: der Metadaten-Suffix gehört in Anführungszeichen —
`"<tabelle>$snapshots"`, nicht `<tabelle>.snapshots` (das ist Spark).

**4. Time Travel**

Snapshot-ID aus Schritt 3 kopieren (19-stellige Zahl, ohne Anführungszeichen
einsetzen):

```sql
SELECT count(*)
FROM nessie.raw.fund_positions FOR VERSION AS OF 3931422776536639655;
```

> **Vorbereitung nötig:** Nach einem frischen `make seed` hat jede Raw-Tabelle
> genau **einen** Snapshot — die Time-Travel-Query läuft, liefert aber denselben
> Stand wie die normale Abfrage. Der Kontrast entsteht erst mit einem zweiten
> Snapshot. Der im Repo ausgearbeitete Weg:
>
> - **Fondspreise:** `./scripts/reset-demo1.sh` lädt Load 1, die Demo-1-Station 3
>   lädt Load 2 dazu → `nessie.raw.fondspreise` hat zwei Snapshots und ist die im
>   [DEMO1-DREHBUCH.md](DEMO1-DREHBUCH.md) ausgearbeitete Time-Travel-Tabelle.
>
> Für die Live-Demo empfiehlt sich, den zweiten Snapshot **vor** der Session zu
> erzeugen und die beiden Snapshot-IDs bereitzulegen.

Weitere Metadaten-Tabellen siehe Spickzettel in Teil B.

---

## Station 5: Transformation — dbt

**Kernbotschaft:** Die Layer-Übergänge sind versionierter SQL-Code mit Tests,
kein Klickpfad.

dbt ist **nur im `jupyter`-Container** installiert (Version 1.11.8, Adapter
`trino` 1.10.1). Alle Aufrufe laufen über den Container-Wrapper:

```bash
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt parse"
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt run"
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test"
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt docs generate"
```

> Die Makefile-Targets `dbt-run`, `dbt-test` und `dbt-docs` rufen
> `cd dbt && uv run dbt ...` auf dem **Host** auf. dbt ist dort nicht
> installiert (`pyproject.toml` enthält kein `dbt-trino`), und
> `dbt/profiles.yml` zeigt auf den Host `trino`, der nur im Docker-Netz
> auflösbar ist. Die drei Targets funktionieren so nicht — die
> Container-Aufrufe oben verwenden.

**Projektstruktur** (`dbt/dbt_project.yml`, Profil `mini_lakehouse` → Target
`dev` → `database: nessie`):

| Layer | Verzeichnis | Schema | Modelle |
|-------|-------------|--------|---------|
| staging | `dbt/models/staging/` | `staging` | `stg_cdp_emissions`, `stg_nzdpu_emissions`, `stg_fondspreise` |
| curated | `dbt/models/curated/` | `curated` | `curated_companies`, `curated_esg_emissions` |
| trusted | `dbt/models/trusted/` | `trusted` | `trusted_esg_emissions` |
| snapshots | `dbt/snapshots/` | `curated` | `snp_fondspreise_scd2` |

Sources sind in `dbt/models/sources.yml` auf `nessie.raw` definiert. Alle Modelle
sind `materialized: table`.

**Für die Demo genügt ein Ausschnitt** — vollständige Referenz inkl. erwarteter
Test-Counts pro Selektor in [DBT-COMMANDS.md](DBT-COMMANDS.md):

```bash
# ESG-Kette bauen und testen
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt run --select curated"
docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt test --select curated"
```

Erwartung für `curated_companies curated_esg_emissions`: **PASS=13/13**.

> `dbt test --select trusted_esg_emissions` zeigt **6 erwartete FAILures** auf
> `not_null scope_1_tco2e`. Das ist gewollt und dokumentiert — die verbindliche
> Prüfung ist das externe Quality Gate.

**Anschlusspunkt DE2 — die dbt-Artefakte.** `dbt docs generate` schreibt nach
`dbt/target/`; das Verzeichnis ist per Volume (`./dbt:/home/jovyan/dbt`) direkt
auf dem Host sichtbar:

| Artefakt | Pfad (Host) | Pfad (Container) |
|----------|-------------|------------------|
| Manifest — Modelle, Tests, Lineage | `dbt/target/manifest.json` | `/home/jovyan/dbt/target/manifest.json` |
| Catalog — Spalten, Typen, Statistiken | `dbt/target/catalog.json` | `/home/jovyan/dbt/target/catalog.json` |
| Run-Ergebnisse | `dbt/target/run_results.json` | `/home/jovyan/dbt/target/run_results.json` |
| Statische Doku-Site | `dbt/target/index.html` | `/home/jovyan/dbt/target/index.html` |

`manifest.json` und `catalog.json` sind die Standard-Schnittstelle für
Metadaten-Ingestion. `index.html` kann direkt per `file://` im Browser geöffnet
werden — `dbt docs serve` ist dafür nicht nötig.

**Quality Gate curated→trusted (nur Kurzhinweis).** Zwischen `curated` und
`trusted` steht ein Great-Expectations-Checkpoint, der die Promotion blockiert,
wenn die Suite rot ist:

```
dbt run --select curated  →  GE checkpoint  →  dbt run --select trusted
                                 ^ blockt Promotion bei FAIL
```

Vorhanden im Repo: Suite `great_expectations/expectations/curated_esg_emissions_suite.json`
(6 Expectations auf `curated_esg_emissions`), Checkpoint
`great_expectations/checkpoints/curated_esg_checkpoint.yml`, Orchestrierung
`scripts/promote-trusted-esg.py` (Exit 0 grün / 1 Gate rot / 2 technischer Fehler).
Ausgearbeitetes Beispiel inkl. rotem Pfad: [DEMO2-QUALITY-GATE.md](DEMO2-QUALITY-GATE.md).
In dieser Demo nur erwähnen, nicht ausführen.

---

## Station 6: Shared Services — PostgreSQL

**Kernbotschaft:** Der Postgres ist nicht nur Nessies Backend, sondern der
Andockpunkt für jedes Tool, das eine eigene Metadaten-Datenbank braucht.

| Parameter | Wert |
|-----------|------|
| Host (vom Host-System) | `localhost:5432` |
| Host (aus Containern) | `postgres:5432` |
| User / Passwort | `lakehouse` / `lakehouse123` |
| Datenbanken | `nessie`, `dagster` |

**Verbindung und Datenbanken listen:**

```bash
docker compose exec postgres psql -U lakehouse -d nessie -c "\l"
```

```
   Name    |   Owner   | Encoding
-----------+-----------+----------
 dagster   | lakehouse | UTF8
 nessie    | lakehouse | UTF8
 postgres  | lakehouse | UTF8
```

`nessie` wird per `POSTGRES_DB` angelegt und hält den Nessie-Version-Store
(`NESSIE_VERSION_STORE_TYPE: JDBC2`). `dagster` legt
`config/postgres/init-databases.sh` beim **ersten** Start des Containers an.

**CREATE DATABASE als Muster** — so dockt ein Gruppenprojekt (MLflow,
OpenMetadata, Superset) mit eigener Metadaten-DB an:

```bash
docker compose exec postgres psql -U lakehouse -d nessie -c "CREATE DATABASE <name>;"
docker compose exec postgres psql -U lakehouse -d nessie -c "\l"
```

Verbindungsstring für ein Tool im selben Docker-Netz (`lakehouse-net`):

```
postgresql://lakehouse:lakehouse123@postgres:5432/<name>
```

Persistent über Container-Neustarts (Volume `postgres-data`), aber **nicht**
über `docker compose down -v` — dabei wird das Volume gelöscht.

Dauerhaft ins Repo gehört eine neue Datenbank in
`config/postgres/init-databases.sh`; dieses Skript läuft allerdings nur beim
Erstinitialisieren des Volumes, ein nachträgliches Ergänzen wirkt erst nach
`docker compose down -v`.

---

## Station 7: Troubleshooting

**Kernbotschaft:** Fast alle Ausfälle in dieser Sandbox haben drei Ursachen —
Zeilenenden, Speicher oder ein Container, der noch nicht healthy ist.

### Zeilenenden (CRLF)

Die Shell-Skripte im Repo müssen LF haben — mit CRLF scheitern sie im
Container mit Meldungen wie `bad interpreter: /bin/bash^M: no such file or directory`.
`.gitattributes` erzwingt das bereits (`* text=auto eol=lf`, `*.sh text eol=lf`).

**Prüfen:**

```bash
git ls-files --eol scripts/
```

Erwartet ist für jede `.sh`-Datei `i/lf    w/lf    attr/text eol=lf` —
`w/crlf` bei einem Shell-Skript ist der Fehlerfall.

**Fix (normalisiert das Working Tree gemäß `.gitattributes`):**

```bash
git add --renormalize .
git status
```

Einzelne Datei ohne git:

```bash
sed -i 's/\r$//' scripts/<datei>.sh
```

### Speicher — `.wslconfig` (Windows)

WSL2 nimmt sich standardmäßig nur 50 % des RAM. `%USERPROFILE%\.wslconfig`
anlegen oder anpassen ([SETUP.md](SETUP.md)):

```ini
[wsl2]
memory=12GB
processors=4
```

Danach zwingend:

```powershell
wsl --shutdown
```

Der Stack braucht laut [README.md](../README.md) mindestens 12 GB für Docker.
Auf 16-GB-Rechnern ist das knapp — nicht benötigte Container stoppen
(Service-Matrix in Teil B).

> Ein `swap=`-Wert ist im Repo nicht dokumentiert. **[TODO: im Repo verifizieren]**
> — falls ein Swap-Wert für die 16-GB-Rechner vorgegeben werden soll, in
> [SETUP.md](SETUP.md) ergänzen und hier nachziehen.

### Speicherdruck erkennen

```bash
docker stats --no-stream
```

Symptom `spark-master` oder `spark-worker` startet und fällt sofort wieder aus →
Docker Desktop → Settings → Resources → Memory auf mindestens 8 GB
([SETUP.md](SETUP.md)). `spark-worker` ist auf `SPARK_WORKER_MEMORY: 2g` /
`SPARK_WORKER_CORES: 2` begrenzt (`docker-compose.yml`).

### Elasticsearch Exit-Code 134

**[TODO: im Repo verifizieren]** — der Stack enthält keinen
Elasticsearch-Service. `docker-compose.yml` definiert `minio`, `postgres`,
`nessie`, `trino`, `spark-master`, `spark-worker`, `jupyter`, `cloudbeaver`,
`trino-init`, `minio-init`; die Zeichenketten „elasticsearch" und „134" kommen
im gesamten Repo nicht vor. Vermutlich betrifft das Fehlerbild eine Komponente
eines Gruppenprojekts (OpenMetadata bringt üblicherweise Elasticsearch mit) und
gehört damit in das jeweilige Gruppen-Briefing, nicht in dieses Dokument.
Exit-Code 134 = SIGABRT, in Container-Kontexten meist ein Out-of-Memory-Abbruch
der JVM — die Behandlung wäre damit ein Speicherthema (siehe oben). Vor dem
Workshop klären, welcher Service konkret gemeint ist.

### Neustart-Sequenzen

```bash
# Einzelnen Service neu starten
docker compose restart trino

# Ganzen Stack neu starten (Daten bleiben erhalten)
docker compose down
docker compose up -d
docker compose ps
# oder:
make restart

# Nur Spark neu starten
docker compose up -d spark-master spark-worker

# Vollständiger Reset — WARNUNG: löscht MinIO- und Postgres-Volumes,
# danach ist ein kompletter make seed nötig
docker compose down -v
docker compose up -d
make seed
# oder:
make clean && make up && make seed
```

Daten-Resets ohne Volume-Löschung:

```bash
./scripts/reset-demo1.sh          # Fondspreise auf Startzustand (Load 1 + Staging + Snapshot)
./scripts/demo2-state.sh raw_cur  # ESG-Pipeline auf Standard-Demo-Setup (ca. 100 s)
./scripts/demo2-state-verify.sh   # Status pro Layer
```

### Weitere bekannte Fehlerbilder

Siehe Troubleshooting-Tabelle in Teil B.

---

## Station 8: Gruppen-Startpunkte

**Kernbotschaft:** Jede Gruppe dockt an genau einer Stelle des bestehenden
Stacks an — der Rest ist schon da.

| Gruppe | Erster Arbeitsschritt am Hackathon-Tag | Benötigte Stationen | Andockpunkt im Stack |
|--------|----------------------------------------|---------------------|----------------------|
| BI | CloudBeaver öffnen, `nessie.trusted.trusted_esg_emissions` abfragen, Verbindungsdaten für das eigene Tool aus Teil B übernehmen | 0, 4 | Trino JDBC `jdbc:trino://trino:8080/nessie` (aus Containern) bzw. `localhost:8080` (vom Host), User frei, kein Passwort |
| DE1 | `make seed` verifizieren, dann eigene Quelldatei nach `data/sample/` legen und Ingestion-Skript aus `scripts/` als Muster kopieren | 0, 1, 2, 3 | Spark → `nessie.raw.<tabelle>`, Bucket `s3a://raw/`; neuer Bucket + Schema nach Muster Station 1 |
| DE2 | `dbt docs generate` im jupyter-Container ausführen, `dbt/target/manifest.json` und `catalog.json` als Input prüfen | 0, 5 | dbt-Artefakte unter `dbt/target/`; Postgres für die eigene Metadaten-DB (Station 6) |
| DS1 | JupyterLab öffnen, `01_iceberg_erkunden.ipynb` durchlaufen, dann eigene Spark-Session per `spark_init.get_spark_session()` | 0, 3 | `spark.table("nessie.<layer>.<tabelle>")` im jupyter-Container; Schreibziel `nessie.staging.*` |
| DS2 | Postgres-Datenbank für die eigene Tracking-/Registry-Komponente anlegen (`CREATE DATABASE`), Bucket für Artefakte anlegen | 0, 1, 6 | `postgresql://lakehouse:lakehouse123@postgres:5432/<name>`; MinIO S3 `http://minio:9000`, Key `lakehouse` / `lakehouse123`; Netz `lakehouse-net` |

Projektinhalte stehen in den Gruppen-Briefings, nicht hier.

---

# Teil B — Referenz

## Ports und Zugangsdaten

Alle Werte aus `.env`; Ports sind dort änderbar (bei Konflikten siehe
[SETUP.md](SETUP.md)).

| Service | URL / Adresse | Port | Zugangsdaten | Zweck |
|---------|---------------|------|--------------|-------|
| MinIO Console | <http://localhost:9001> | 9001 | `lakehouse` / `lakehouse123` | Buckets und Objekte im Browser |
| MinIO API (S3) | `http://localhost:9000` (Host), `http://minio:9000` (Container) | 9000 | Key `lakehouse` / Secret `lakehouse123`, Region `us-east-1`, Path-Style | S3-Endpoint für Engines und Tools |
| PostgreSQL | `localhost:5432` (Host), `postgres:5432` (Container) | 5432 | `lakehouse` / `lakehouse123` | Nessie-Backend; DBs `nessie`, `dagster` |
| Nessie | <http://localhost:19120> | 19120 | keine | Iceberg-Katalog; API v2 (Spark), v1 (Trino); Branch `main` |
| Trino Web UI | <http://localhost:8080> | 8080 | beliebiger Username, kein Passwort | SQL-Engine, Query-Monitoring |
| Trino JDBC | `jdbc:trino://trino:8080/nessie` | 8080 | User frei (`dbt`, `trino`, `init` in Verwendung) | Katalog `nessie` |
| Spark Master UI | <http://localhost:8081> | 8081 | keine | Cluster-Status |
| Spark Master | `spark://spark-master:7077` | 7077 | keine | `spark-submit`-Ziel |
| JupyterLab | <http://localhost:8888?token=lakehouse> | 8888 | Token `lakehouse` | Notebooks, dbt, Spark (`local[*]`) |
| CloudBeaver | <http://localhost:8978> | 8978 | Admin-Passwort beim Erststart selbst setzen | SQL-Editor; Verbindung „Lakehouse (Trino)" vorkonfiguriert |

Docker-Netz: `lakehouse-net` (Compose-Name `mini-lakehouse_lakehouse-net`).
Container erreichen sich über ihre Service-Namen. Volumes: `minio-data`,
`postgres-data`, `cloudbeaver-data`.

## Service-Matrix

Welche Container welche Gruppe braucht — relevant für 16-GB-Rechner. `X` = nötig,
`–` = kann gestoppt werden.

| Container | BI | DE1 | DE2 | DS1 | DS2 | Anmerkung |
|-----------|----|----|----|----|----|-----------|
| `minio` | X | X | X | X | X | Storage — ohne läuft nichts |
| `postgres` | X | X | X | X | X | Nessie-Backend; ohne fällt der Katalog aus |
| `nessie` | X | X | X | X | X | Katalog — ohne läuft nichts |
| `trino` | X | X | X | X | – | Pflicht für SQL und dbt |
| `jupyter` | – | X | X | X | X | enthält dbt **und** Notebooks |
| `spark-master` | – | X | – | – | – | nur für `spark-submit` / `make seed` |
| `spark-worker` | – | X | – | – | – | nur für `spark-submit` / `make seed` |
| `cloudbeaver` | X | – | – | – | – | reines Komfort-UI; Trino-CLI ist die Alternative |

Notebooks nutzen Spark im Modus `local[*]` **im jupyter-Container** — DS-Gruppen
brauchen `spark-master`/`spark-worker` daher nicht. Nach `make seed` können beide
gestoppt werden; das spart auf 16-GB-Rechnern spürbar Speicher:

```bash
docker compose stop spark-master spark-worker
docker compose stop cloudbeaver
docker compose start spark-master spark-worker   # wenn make seed erneut nötig
```

Einzelne Services gezielt starten:

```bash
docker compose up -d minio postgres nessie trino jupyter
```

## Iceberg-SQL-Spickzettel (Trino-Syntax)

Metadaten-Tabellen brauchen in Trino Anführungszeichen um
`<tabelle>$<metadaten>`; in Spark ist es Punkt-Notation
(`nessie.raw.fund_positions.snapshots`).

| Zweck | Query |
|-------|-------|
| Layer / Schemas | `SHOW SCHEMAS IN nessie;` |
| Tabellen eines Layers | `SHOW TABLES IN nessie.raw;` |
| Spalten und Typen | `DESCRIBE nessie.trusted.trusted_esg_emissions;` |
| Vollständiges DDL inkl. Location | `SHOW CREATE TABLE nessie.raw.fund_positions;` |
| Snapshots (Versionshistorie) | `SELECT * FROM nessie.raw."fund_positions$snapshots";` |
| History (Ancestry der Snapshots) | `SELECT * FROM nessie.raw."fund_positions$history";` |
| Data Files (Pfad, Zeilen, Größe) | `SELECT * FROM nessie.raw."fund_positions$files";` |
| Manifests | `SELECT * FROM nessie.raw."fund_positions$manifests";` |
| Partitionen | `SELECT * FROM nessie.raw."fund_positions$partitions";` |
| Spalten über alle Tabellen | `SELECT table_name, column_name, data_type FROM nessie.information_schema.columns WHERE table_schema = 'trusted';` |

**Snapshots mit den relevanten Feldern:**

```sql
SELECT snapshot_id,
       committed_at,
       operation,
       summary['added-records'] AS added_records,
       summary['total-records'] AS total_records
FROM nessie.raw."fund_positions$snapshots"
ORDER BY committed_at;
```

**Data Files — die Parquet-Pfade auf MinIO:**

```sql
SELECT file_path,
       record_count,
       file_size_in_bytes
FROM nessie.raw."fund_positions$files";
```

**Time Travel** — Snapshot-ID (19-stellig) aus `$snapshots`, ohne Anführungszeichen:

```sql
SELECT count(*)
FROM nessie.raw.fund_positions FOR VERSION AS OF 3931422776536639655;
```

Zwei Stände vergleichen:

```sql
SELECT 'snap1' AS stand, count(*) AS rows FROM nessie.raw.fondspreise FOR VERSION AS OF <SNAP1>
UNION ALL
SELECT 'snap2', count(*) FROM nessie.raw.fondspreise FOR VERSION AS OF <SNAP2>;
```

> `nessie.raw.fondspreise` hat nach Demo-1-Load-2 zwei Snapshots. Nach reinem
> `make seed` hat jede Raw-Tabelle nur einen — siehe Hinweis in Station 4.

**Spark-Äquivalente:**

```python
spark.sql("SELECT * FROM nessie.raw.fund_positions.snapshots").show()
spark.sql("SELECT * FROM nessie.raw.fund_positions.files").show(truncate=False)
spark.sql("SELECT * FROM nessie.raw.fund_positions.manifests").show(truncate=False)
spark.sql("SELECT * FROM nessie.raw.fund_positions.partitions").show()
spark.sql("DESCRIBE EXTENDED nessie.raw.fund_positions").show(50, truncate=False)
spark.sql("SELECT * FROM nessie.raw.fund_positions VERSION AS OF 3931422776536639655").count()
```

## Troubleshooting-Tabelle

| Symptom | Ursache | Fix |
|---------|---------|-----|
| `bad interpreter: /bin/bash^M` | Skript hat CRLF statt LF | `git add --renormalize .` — Prüfung: `git ls-files --eol scripts/` |
| `spark-submit` scheitert mit Path-Fehler / „no such file" | Git-Bash-Pfadkonvertierung | `MSYS_NO_PATHCONV=1` voranstellen oder in PowerShell ausführen |
| `spark-master`/`spark-worker` startet und fällt sofort aus | Zu wenig Docker-RAM | Docker Desktop → Resources → Memory ≥ 8 GB; Windows: `.wslconfig` `memory=12GB`, dann `wsl --shutdown` |
| `docker compose exec: not running` | Stack nicht gestartet | `docker compose up -d` |
| `docker compose up` scheitert, Port belegt | Port-Konflikt auf dem Host | Port in `.env` ändern (z. B. `TRINO_PORT=18080`), dann `docker compose up -d` |
| Trino nicht erreichbar | Container noch nicht healthy | `docker compose up -d && docker compose ps` — healthy abwarten (60–90 s) |
| `SHOW SCHEMAS IN nessie` zeigt `staging`/`curated`/`trusted` nicht | Init-Schemas fehlen | `bash scripts/init-schemas.sh` im `trino`-Container-Kontext oder Stack neu starten |
| dbt: „table not found" | Upstream-Layer nicht gebaut | `./scripts/demo2-state.sh raw_cur` bzw. `./scripts/reset-demo1.sh` |
| `dbt run`/`dbt test` schlägt auf dem Host fehl | dbt ist nur im `jupyter`-Container installiert | `docker compose exec jupyter bash -c "cd /home/jovyan/dbt && dbt ..."` |
| `dbt test --select trusted_esg_emissions` zeigt 6 FAIL | Erwartetes Verhalten (`not_null scope_1_tco2e`) | Kein Fix — Quality Gate ist die verbindliche Prüfung |
| `dbt snapshot` zeigt 0 Änderungen | Staging nicht refreshed | Erst `dbt run --select stg_fondspreise`, dann `dbt snapshot` |
| `raw.fondspreise` leer | Demo-1-Daten nicht geladen | `./scripts/reset-demo1.sh` |
| Time-Travel-Query: „snapshot not found" | Veraltete/falsche Snapshot-ID | ID neu aus `"<tabelle>$snapshots"` kopieren |
| Time Travel zeigt keinen Unterschied | Nur ein Snapshot vorhanden | Zweiten Snapshot erzeugen (Demo-1-Load-2 auf `raw.fondspreise`) |
| CloudBeaver verbindet nicht | Stale Connection | Connection rechtsklick → *Invalidate* |
| Cross-Source-Query liefert nur 1 Zeile | ISIN nicht in beiden Quellen | Andere DAX-ISIN wählen oder Reset |
| GE-Checkpoint: „table does not exist" | Curated nicht gebaut | `./scripts/demo2-state.sh raw_cur` bzw. `dbt run --select curated` |
| `UnicodeEncodeError` bei GE-/Promotion-Skript | Windows-Terminal nutzt cp1252 | PowerShell: `$env:PYTHONIOENCODING = "utf-8"` **vor** dem Aufruf (Inline-Form `VAR=... cmd` funktioniert in PowerShell nicht); Bash: `export PYTHONIOENCODING=utf-8` |
| GE Phase 2 rot ohne Manipulation | Demo-INSERT nicht aufgeräumt | `DELETE FROM nessie.curated.curated_esg_emissions WHERE ingestion_id = 'demo-ge-violation';` |
| `make health` schlägt fehl | `scripts/healthcheck.sh` existiert nicht | `docker compose ps` verwenden |
| Container weg nach Neustart, Daten fehlen | `docker compose down -v` gelöscht die Volumes | `docker compose up -d && make seed` |
