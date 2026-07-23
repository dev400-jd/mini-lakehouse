# Demo 3 Drehbuch — MLflow & Data Science (Fonds-CO₂-Fußabdruck)

**Ziel:** Zeigen, wie ein klassisches Data-Science-Projekt (Fonds-CO₂-Fußabdruck +
ESG-Clustering) **nativ in den bestehenden Lakehouse-Stack** eingebettet ist —
mit dem durchgängigen MLOps-Bogen *Daten → Training → Experiment-Vergleich →
Registry → Reporting*, alles ohne neue Backing-Services.

**Dauer:** ca. 10–12 Minuten Live-Demo (+ ca. 3 Minuten Setup vor dem Vortrag).

**Werkzeuge:** Folien (Folie 1–3), Terminal (Bash, Repo-Root), Browser mit
MLflow-UI (`http://localhost:5555`) und Jupyter (`http://localhost:8888?token=lakehouse`),
Docker Desktop mit laufendem Stack.

> Kein Vorlese-Skript. Die **Sprech-Anker** tragen den Hauptgedanken der Station —
> in eigenen Worten ausformulieren.

---

## Vor der Demo

Etwa 3 Minuten vor Demo-Start:

1. **Stack prüfen**
   ```bash
   docker compose ps            # alle Services "healthy", inkl. mlflow
   ```
2. **Sauberen Startzustand setzen** (idempotent, ~30 Sekunden)
   ```bash
   bash scripts/reset-mlflow-demo.sh
   ```
   Danach ist MLflow leer. **`make train` NICHT vorab ausführen** — das ist Station 3.
3. **Anwendungen vorbereiten**
   - [ ] Folie 1 (Architektur) als Startbild
   - [ ] Browser-Tab MLflow-UI offen: `http://localhost:5555` (noch leer)
   - [ ] Terminal in Repo-Root, große Schrift, `export MSYS_NO_PATHCONV=1` (nur Windows/Git-Bash)
   - [ ] Fallback-Screenshots griffbereit (siehe unten)
   - [ ] Notifications stumm

> Wenn etwas hakt: `bash scripts/reset-mlflow-demo.sh` erneut ausführen — das Skript
> ist auf wiederholten Aufruf ausgelegt.

---

## Stationen-Übersicht

| #  | Station                          | Bildschirm            | Dauer    |
|----|----------------------------------|-----------------------|----------|
| 1  | Warum & Architektur              | Folie 1–2             | 2 Min    |
| 2  | „Kein neuer Service"             | Terminal              | 1 Min    |
| 3  | Training live starten            | Terminal              | 2 Min    |
| 4  | Runs vergleichen                 | MLflow-UI             | 2–3 Min  |
| 5  | Artefakte: Modell, Plot, Report  | MLflow-UI             | 1–2 Min  |
| 6  | Go-Live: Model Registry          | MLflow-UI             | 2 Min    |
| 7  | MLOps-Bogen & Ausblick           | Folie 3               | 1 Min    |
|    |                                  | **Summe**             | **11–12 Min** |

---

## Station 1 — Warum & Architektur (Folie 1–2)

**Was passiert:** Einstieg über die Folien. Ausgangslage: der Lakehouse-Stack steht
(Iceberg, Nessie, Spark, Trino, MinIO). Frage: Wo kommt Machine Learning rein?

**Sprech-Anker:**
- „MLflow braucht genau zwei Dinge, die unser Lakehouse schon hat: eine Datenbank
  und einen Objektspeicher. Also haben wir nichts Neues gebaut — sondern PostgreSQL
  und MinIO wiederverwendet."
- „Backend-Store = PostgreSQL (Runs, Metriken, Parameter), Artifact-Store = MinIO
  (Modelle, Plots). Die MLflow-UI läuft auf Port 5555."

---

## Station 2 — „Kein neuer Service" (Terminal)

**Was passiert:** `docker compose ps` zeigen — `mlflow` steht als healthy Service
neben den bekannten.

```bash
docker compose ps
```

**Sprech-Anker:**
- „Ein zusätzlicher Container `mlflow` — Backend und Artefakte laufen über die
  bestehenden Postgres- und MinIO-Container. Das ist die ganze Infrastruktur."

---

## Station 3 — Training live starten (Terminal)

**Was passiert:** Das Training anstoßen. Die k-Sweep-Ausgabe kommentieren.

```bash
make train
```

Erwartete Ausgabe:
```
Features geladen: 10 Fonds, 5 Features
  k=2: silhouette=0.266, inertia=32.5
  k=3: silhouette=0.186, inertia=23.8
  k=4: silhouette=0.177, inertia=17.6
  k=5: silhouette=0.191, inertia=10.5
  k=6: silhouette=0.179, inertia= 5.9
Bestes Modell: k=2 (silhouette=0.266)
```

**Sprech-Anker:**
- „Die Features kommen direkt aus dem See: pro Fonds die **gewichteten
  CO₂-Emissionen** seiner Holdings — ein Trino-Join von Fondspositionen auf
  Unternehmensemissionen über die ISIN."
- „Wir probieren mehrere Cluster-Anzahlen `k` durch. **Jeder Lauf** wird als
  MLflow-Run protokolliert — Parameter, Metriken, Modell, Plot."

---

## Station 4 — Runs vergleichen (MLflow-UI)

**Was passiert:** `http://localhost:5555` neu laden → Experiment
**`fonds-co2-fussabdruck`** → Runs-Tabelle.

1. Spalten zeigen: `k`, `silhouette`, `inertia`.
2. Nach **`silhouette` absteigend** sortieren → `kmeans_k2` steht oben.
3. Alle `kmeans_*`-Runs markieren → **Compare** → Metrik-Charts / Parallel Coordinates.

**Sprech-Anker:**
- „Hier vergleichen wir die Läufe objektiv statt nach Bauchgefühl. Die **Inertia**
  fällt monoton mit mehr Clustern — die **Silhouette** hat ihr Maximum bei k=2."
- „Genau das ist die Kernaussage: **mehr Cluster ist nicht automatisch besser.**
  MLflow macht diesen Trade-off sichtbar und reproduzierbar."

---

## Station 5 — Artefakte: Modell, Plot, Report (MLflow-UI)

**Was passiert:** Einen Run öffnen (z. B. `kmeans_k2`) → Tab **Artifacts**.

- `model/` — das serialisierte KMeans-Modell (ladbar, versioniert)
- `plots/clusters_k2.png` — der Cluster-Scatter (Fonds nach Emissionen × Energy-Exposure)
- im Run `final_labeled_k2`: `report/fonds_co2_report.csv` — die gelabelte Fondsliste

**Sprech-Anker:**
- „Alle Artefakte liegen in MinIO — dieselbe S3-Schicht wie unsere Iceberg-Daten.
  Das Modell ist kein loses Pickle auf einem Laptop, sondern versioniert am Run."
- „Der Report zeigt: Cluster 0 = CO₂-intensivere Fonds, Cluster 1 = emissionsärmere."

---

## Station 6 — Go-Live: Model Registry (MLflow-UI)

**Was passiert:** Oben **Models** → **`fonds-esg-clustering`** → Version 1 öffnen →
Stage auf **Production** setzen.

**Sprech-Anker:**
- „Trainieren heißt nicht deployen. Ein Modell geht erst live, wenn wir es hier
  bewusst auf **Production** heben — versioniert, mit Historie, jederzeit
  zurückrollbar."
- „Das ist unser Deployment-Schalter: `trainieren → registrieren → promoten`. Ein
  FastAPI-Service davor würde immer die Production-Version servieren."

---

## Station 7 — MLOps-Bogen & Ausblick (Folie 3)

**Sprech-Anker:**
- „Der ganze Bogen lief **innerhalb des bestehenden Stacks**: Iceberg/Trino →
  Feature-Engineering → Training mit Vergleich → Registry → Reporting."
- „Nächste Schritte: Batch-Scoring der Cluster zurück in eine Iceberg-Tabelle
  `curated.fund_esg` (in CloudBeaver abfragbar) und ein FastAPI-Endpoint fürs
  Serving."

---

## Fallback (wenn die Live-Demo hakt)

- **`make train` schlägt fehl / hängt:** Screenshots der Terminal-Ausgabe zeigen und
  direkt in die (vorbefüllte) MLflow-UI wechseln — der sauberе Reset lässt sich auch
  vorab einmal durchlaufen, dann sind die 6 Runs schon da.
- **MLflow-UI nicht erreichbar:** `docker compose up -d mlflow` und
  `docker exec jupyter curl -sf http://mlflow:5000/health` (muss `OK` sein).
- **Ganz sichergehen:** Vor dem Vortrag einmal komplett durchspielen
  (`reset-mlflow-demo.sh` → `make train` → UI-Tour) und Screenshots jeder Station
  ablegen.
