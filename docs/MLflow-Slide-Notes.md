# Slide-Notizen & Modelldokumentation — MLflow-Workshop

Begleitnotizen zu `docs/MLflow-Workshop.pptx` (3 Folien). Pro Folie die Sprech-Anker,
danach die fachliche Dokumentation der Modellergebnisse und -vorhersagen.

---

## Folie 1 — MLflow im Mini-Lakehouse (Architektur)

**Kernbotschaft:** Machine Learning wurde in den bestehenden Stack integriert, ohne
neue Backing-Services einzuführen.

- MLflow braucht nur zwei Dinge — eine Datenbank und einen Objektspeicher —, und beides
  hat das Lakehouse schon: **PostgreSQL** als Backend-Store (Runs, Parameter, Metriken)
  und **MinIO** als Artifact-Store (Modelle, Plots, Reports).
- Der Datenfluss: Trainingsdaten kommen aus dem **Iceberg-`raw`-Layer via Trino**, das
  Training läuft in **Jupyter**, protokolliert wird gegen den **MLflow Tracking Server**
  (UI auf Port 5555).
- Ein zusätzlicher Container `mlflow` — mehr Infrastruktur war nicht nötig.

---

## Folie 2 — Fonds-CO₂-Fußabdruck & ESG-Clustering (Projekt)

**Kernbotschaft:** Ein durchgängiges DS-Projekt aus Lakehouse-Daten, in vier Schritten.

- **Feature-Engineering:** Trino-Join der Fondspositionen auf die Unternehmens-Emissionen
  über die ISIN, gewichtet mit dem Positionsgewicht (`weight_pct`, summiert je Fonds auf 100 %).
- **KMeans mit k-Sweep:** k = 2…6, standardisierte Features (KMeans ist distanzbasiert).
- **MLflow-Tracking:** jeder Lauf wird protokolliert — Parameter, Silhouette, Inertia, Modell, Plot.
- **Model Registry:** das beste Modell wird versioniert und ist promotebar.
- Datengrundlage bewusst benennen: **10 Fonds, 30 Unternehmen, 100 % ISIN-Coverage** — jede
  Position hat Emissionsdaten. Features: `n_holdings`, `w_scope1`, `w_scope2`,
  `pct_energy`, `pct_materials`.

> Ehrlicher Hinweis fürs Publikum: 10 Fonds sind für *überwachtes* Lernen zu wenig — deshalb
> **unüberwachtes Clustering**. Das passt zur Datengröße und zeigt MLflow trotzdem voll.

---

## Folie 3 — Ergebnis & der MLOps-Bogen

**Kernbotschaft:** Das Optimum liegt bei k = 2; Go-Live läuft über die Registry.

- **Modellwahl:** Die **Inertia** fällt monoton mit mehr Clustern (mehr Zentren = kompaktere
  Gruppen), die **Silhouette** hat ihr Maximum bei **k = 2 (0,266)**. Aussage: *mehr Cluster
  ist nicht automatisch besser* — MLflow macht diesen Trade-off objektiv sichtbar.
- **Go-Live-Workflow:** `trainieren → registrieren → promoten (Production) → servieren`. Ein
  Modell geht erst live, wenn es bewusst auf Stage *Production* gehoben wird — versioniert und
  zurückrollbar.
- **Ausblick:** Batch-Scoring der Cluster zurück in eine Iceberg-Tabelle `curated.fund_esg`
  (dann in CloudBeaver per SQL abfragbar) und ein FastAPI-Endpoint fürs Serving.

---

## Modellergebnisse & Vorhersagen (fachliche Dokumentation)

### Was das Modell misst

Pro Fonds wird ein **gewichteter Emissions-Fußabdruck** berechnet:
`w_scope1 = Σ (Positionsgewicht% × Scope-1-Emissionen des Emittenten)`, analog `w_scope2`.
Dazu das Exposure in CO₂-intensiven Sektoren (`pct_energy`, `pct_materials`) und die Anzahl
Positionen. Diese fünf Merkmale gehen standardisiert in ein KMeans-Clustering (k = 2).

> **Wichtige fachliche Einordnung:** `w_scope1` ist ein **Financed-Emissions-Proxy** (absolute,
> gewichtete Emissionsmenge) — **keine normierte Karbonintensität** (WACI in tCO₂e/€ Umsatz),
> weil uns finanzielle Nenner (Umsatz/EVIC) fehlen. Große Emittenten dominieren daher das Bild.
> Für ein produktives ESG-Reporting (SFDR/TCFD) wäre die Normierung der nächste Schritt.

### Die Vorhersagen (k = 2)

| Fonds | Typ | Positionen | w_scope1 | w_scope2 | Energy % | Materials % | **Cluster** |
|-------|-----|-----------:|---------:|---------:|---------:|------------:|:-----------:|
| Deka-Nachhaltigkeit Renten CF | Rentenfonds | 17 | 12,03 M | 373 592 | 21,6 | 11,1 | **0** |
| Deka-ESG MSCI World Climate Paris CF | Indexfonds | 19 | 9,54 M | 370 439 | 17,5 | 5,1 | **0** |
| DekaFonds CF | Aktienfonds | 16 | 9,16 M | 323 910 | 15,8 | 5,2 | **0** |
| Deka-Wandelanleihen CF | Mischfonds | 16 | 7,62 M | 373 435 | 11,1 | 9,5 | **0** |
| Deka-EuropaSelect CF | Aktienfonds | 19 | 6,92 M | 377 101 | 12,4 | 3,2 | **0** |
| Deka-Klimawandel & Biodiversitaet CF | Thematischer Fonds | 18 | 6,81 M | 356 005 | 10,3 | 8,7 | **0** |
| Deka-GlobalChampions CF | Aktienfonds | 18 | 6,46 M | 402 781 | 10,0 | 9,9 | **0** |
| Deka-Nachhaltigkeit Aktien CF | Aktienfonds | 13 | 5,31 M | 337 796 | 9,9 | 0,0 | **1** |
| Deka-Europa Aktien Spezial CF | Aktienfonds | 20 | 5,19 M | 288 110 | 7,7 | 6,5 | **1** |
| Deka-Basisstrategie Aktien CF | Aktienfonds | 13 | 4,80 M | 301 224 | 4,9 | 11,1 | **1** |

### Interpretation der Cluster

- **Cluster 0 — „CO₂-intensiver" (7 Fonds):** Ø `w_scope1` ≈ **8,4 M**, Ø Energy-Exposure ≈ **14 %**.
- **Cluster 1 — „emissionsärmer" (3 Fonds):** Ø `w_scope1` ≈ **5,1 M**, Ø Energy-Exposure ≈ **7,5 %**.
- Cluster 0 trägt also rund **60 % mehr** gewichtete Scope-1-Emissionen und etwa das **Doppelte**
  an Energy-Exposure. Die Trennlinie verläuft primär entlang `w_scope1` (Grenze ≈ 5,9 M);
  Energy-Exposure korreliert, `w_scope2` und `pct_materials` tragen kaum zur Trennung bei.

### Fachlich bemerkenswerte Vorhersagen (gute Diskussionspunkte)

- **„Deka-Nachhaltigkeit Renten CF" hat den höchsten Fußabdruck** (12,0 M, 21,6 % Energy) —
  trotz „Nachhaltigkeit" im Namen. Erklärung: Rentenfonds halten oft kapitalintensive
  Versorger/Energie-Emittenten; der **Name ≠ der gemessene Fußabdruck**.
- **„ESG MSCI World Climate Paris CF" liegt im CO₂-intensiven Cluster** (9,5 M). Auch ein
  Paris-aligned-Klimaprodukt trägt in **absoluten, gewichteten** Emissionen spürbar bei —
  ein starkes Argument dafür, den Fußabdruck zu *messen* statt aus dem Label zu schließen.
- **Die drei emissionsärmsten Fonds sind allesamt Aktienfonds**; „Deka-Basisstrategie Aktien"
  ist mit 4,8 M und nur 4,9 % Energy-Exposure der „grünste" im Portfolio.

### Grenzen der Aussage (transparent benennen)

- **10 Fonds / 30 Emittenten, synthetische Beispieldaten** → das Clustering ist **illustrativ**,
  nicht statistisch belastbar. Die Silhouette von 0,266 zeigt eine erkennbare, aber moderate
  Trennschärfe.
- `w_scope1` ist ein **Proxy** (s. o.), keine umsatznormierte Intensität.
- Scope-2 (market-based) ist über die Fonds relativ flach → geringe Trennwirkung.

### Fachlicher Nutzen

Ein solches Clustering ist die Basis für **ESG-Risiko-Tiering**, aufsichtsrechtliches
**SFDR-/TCFD-Reporting**, **Dekarbonisierungsziele** je Fonds und **Transparenz gegenüber
Anlegern** — und dank Lakehouse reproduzierbar aus den Rohdaten ableitbar.
