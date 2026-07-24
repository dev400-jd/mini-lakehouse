# OpenMetadata im mini-lakehouse — Zusammenfassung für die Präsentation

## 1. Kontext & Zielsetzung (DE2-Teilprojekt)

OpenMetadata wurde als **operativer Metadaten-Katalog** neben den bestehenden Kern-Stack gesetzt, referenziert als eigenständiges Hackathon-Teilprojekt „DE2" (siehe `DE-2-Hackathon-Projekt.docx`, extern, nicht im Repo). Kernidee: Der Katalog soll sichtbar machen, was im Lakehouse an Tabellen existiert, wie diese durch die Medaillon-Schichten (raw → staging → curated → trusted) fließen, und was dbt als Tests/Dokumentation dazu weiß — **ohne** den bestehenden produktiven Stack anzufassen.

Bewusste Architekturentscheidung: **eigenes Compose-File** `docker-compose.openmetadata.yml`, per `-f` zusätzlich zum Basis-`docker-compose.yml` geladen, statt es dort einzubauen. Begründung im Header-Kommentar der Datei: der Kern-Stack bleibt unangetastet, dieser Teil ist unabhängig start-/stoppbar — wichtig für eine Demo, bei der man OpenMetadata gezielt zu- und wieder abschalten will, ohne Spark/Trino/Nessie neu zu starten.

---

## 2. Installation & Infrastruktur

### 2.1 Vier Services, geteilte Ressourcen

Statt eine komplett eigene Infrastruktur mitzubringen (was OpenMetadata standardmäßig tut — inkl. eigenem MySQL/Postgres und Airflow), wurde bewusst **maximal auf den bestehenden Stack aufgesetzt** ("Hebel 1" laut internem Briefing):

| Service | Rolle | Lebensdauer |
|---|---|---|
| `openmetadata-db-init` | legt DB `openmetadata_db` im **bestehenden** Postgres 16 an | einmalig |
| `elasticsearch` | Suchindex — Pflicht für Lineage-Graph, Volltextsuche, Glossar-Lookup | dauerhaft |
| `openmetadata-migrate` | Flyway-Schema-Migration + Suchindex-Mapping | einmalig |
| `openmetadata-server` | Anwendung + UI, Port 8585 (App) / 8586 (Admin/Health) | dauerhaft |

Kein eigenes MySQL/Postgres für OpenMetadata (nutzt die vorhandene Postgres-Instanz mit eigener Datenbank `openmetadata_db`), kein Airflow-Ingestion-Container (`PIPELINE_SERVICE_CLIENT_ENABLED: "false"`) — Ingestion läuft stattdessen komplett per CLI aus dem bestehenden `jupyter`-Container heraus, der ohnehin Netzwerkzugriff auf Trino und den dbt-Projektordner hat.

Start-Reihenfolge über `depends_on`: `db-init` → `elasticsearch` → `migrate` → `server`.

```bash
docker compose -f docker-compose.yml -f docker-compose.openmetadata.yml up -d openmetadata-server
docker compose -f docker-compose.yml -f docker-compose.openmetadata.yml \
  rm -sf openmetadata-server elasticsearch openmetadata-db-init openmetadata-migrate   # sauberer Abbau
```

### 2.2 Zwei reale Probleme beim Erst-Setup (praktisch verifiziert, 2026-07-23)

Diese beiden Punkte lohnen sich für die Präsentation als "was in der Praxis überrascht hat":

1. **Server crasht in Restart-Schleife** mit `relation "openmetadata_settings" does not exist`. Ursache: Das offizielle `openmetadata-server`-Image führt die Flyway-Migration **nicht automatisch** beim Start aus — das ist eine naheliegende, aber falsche Annahme. Lösung: eigener `openmetadata-migrate`-Init-Container, der denselben Bootstrap-Wrapper (`openmetadata-ops.sh migrate`) einmalig vor dem Server-Start ausführt.
2. **Healthcheck bleibt dauerhaft `unhealthy`**, obwohl die UI unter Port 8585 normal erreichbar ist. Ursache: Das Server-Image ist Alpine-basiert und enthält **kein `curl`** (anders als die übrigen Services im Compose-File) → Healthcheck musste auf `wget -q --spider` umgestellt werden.

Beide Fixes sind bereits fest in `docker-compose.openmetadata.yml` hinterlegt, nicht mehr manuell nötig.

### 2.3 Weitere Konfigurationsdetails

- JVM-Heap explizit begrenzt (`OPENMETADATA_HEAP_OPTS: -Xmx1G -Xms1G`) — Ressourcenschonung für die Sandbox-Umgebung.
- Elasticsearch als Single-Node, Security deaktiviert, Heap auf 512m begrenzt — bewusst minimal, kein produktionsreifes Setup.
- Login: `admin@open-metadata.org` / `admin` (Achtung: Login läuft über die **E-Mail**, nicht den Benutzernamen — `admin` allein wird als ungültige E-Mail interpretiert und kann ins Self-Signup-Flow führen und einen neuen Nicht-Admin-User anlegen).
- Alle Image-Versionen/Ports zentral über `.env` (`OPENMETADATA_VERSION=1.5.11`, `ELASTICSEARCH_VERSION=8.11.4`, `OPENMETADATA_DB`, `OPENMETADATA_SERVER_PORT=8585`, `OPENMETADATA_ADMIN_PORT=8586`, `ELASTICSEARCH_PORT=9200`).
- Offener Punkt: Die gewählte Version (`1.5.11`) und die verwendeten Env-Var-Namen wurden **nicht** gegen die aktuelle offizielle Release-Doku (get.openmetadata.org) gegengeprüft — als Vorbehalt vor dem eigentlichen Hackathon-Tag dokumentiert.

---

## 3. Ingestion — wie die Metadaten tatsächlich in den Katalog kommen

Kein Airflow-basierter Ingestion-Container, sondern ein **zweistufiger CLI-Workflow**, ausgeführt im `jupyter`-Container:

### Schritt 1 — Trino-Connector: Tabellen registrieren (`ingest-trino.yaml`)

Registriert die vier Schemas `raw`/`staging`/`curated`/`trusted` (gefiltert per `schemaFilterPattern`) aus dem `nessie`-Catalog über Trino als OpenMetadata-**Database-Service** namens `trino_lakehouse`. Das ist der Schritt, der die Tabellen-Entities überhaupt erst anlegt.

### Schritt 2 — dbt-Ingestion: Lineage/Docs/Tests anhängen (`ingest-dbt.yaml`)

Liest `dbt/target/manifest.json`, `catalog.json` und `run_results.json` und hängt daraus **Lineage-Graph, Column-Descriptions und Testergebnisse** an die in Schritt 1 bereits angelegten Tabellen. Wichtig: dbt-Ingestion legt selbst **keine** neuen Tabellen an — sie reichert nur existierende Entities an. Deshalb ist die Reihenfolge zwingend: erst Trino, dann dbt.

Beide YAMLs injizieren das Auth-Token über `${OPENMETADATA_JWT_TOKEN}` per `envsubst` zur Laufzeit statt es fest im Repo zu hinterlegen (gleiches Muster wie bei der Spark-Konfiguration in `config/jupyter/before-spark-conf.sh`).

### Praktischer Ablauf

1. **JWT-Token besorgen**: UI → Settings → Bots → `ingestion-bot` → Token generieren, lokal als `OPENMETADATA_JWT_TOKEN` exportieren.
2. **Ingestion-Package in eigenem venv installieren** (nicht im Haupt-Python des Jupyter-Containers!):
   ```bash
   python3 -m venv /home/jovyan/.venvs/om-ingest
   /home/jovyan/.venvs/om-ingest/bin/pip install 'openmetadata-ingestion[trino,dbt]==1.5.11'
   ```
   Zwei harte Gründe für das separate venv (beide praktisch verifiziert):
   - **Versions-Kopplung Client/Server**: ohne Pin zieht pip die neueste PyPI-Version, die dann mit dem Server (`1.5.11`) inkompatibel ist und sofort mit einem Versions-Mismatch-Fehler abbricht.
   - **Package-Konflikt mit der bestehenden dbt-Pipeline**: Das `dbt`-Extra von `openmetadata-ingestion` zieht ein anderes `dbt-core`/`dbt-adapters` und überschreibt die vom `jupyter`-Dockerfile installierte `dbt-trino`-Kette — die eigentliche dbt-Pipeline wird dadurch kaputt. Repariert man das nachträglich, zieht das wiederum neuere `sqlfluff`/`protobuf`-Versionen, die `metadata` selbst mit einem Import-Error zerschießen. Ein eigenes venv trennt beide Toolchains sauber und umgeht das Problem komplett.
3. **Trino-Tabellen registrieren** (`metadata ingest -c ingest-trino.yaml`).
4. **`catalog.json` erzeugen** — entsteht **nicht** bei normalem `dbt run`/`dbt build`, sondern nur bei `dbt docs generate`. Ohne diese Datei bekommt man zwar Lineage, aber keine Column-Level-Details.
5. **dbt-Artefakte einspielen** (`metadata ingest -c ingest-dbt.yaml`).
6. **Verifikation in der UI**: Explore → `trino_lakehouse` → eine Tabelle in `trusted` öffnen → Tab **Lineage** zeigt die Kette raw → staging → curated → trusted, Tab **dbt** zeigt die Testergebnisse aus `run_results.json`.

**Ergebnis des verifizierten Testlaufs (2026-07-23)**: Trino-Ingestion 20 Records / 0 Fehler, dbt-Ingestion 168 Records / 0 Fehler, jeweils 100 % Erfolgsquote.

---

## 4. Was dbt konkret zuliefert

dbt ist hier nicht nur Transformationswerkzeug, sondern die **primäre Metadaten-Quelle** für alles, was über die reine Tabellenstruktur hinausgeht:

- **Lineage-Graph**: aus `manifest.json` — welches Modell hängt von welcher Quelle/welchem anderen Modell ab, sichtbar als Graph raw → staging → curated → trusted.
- **Column-Level-Beschreibungen**: aus den `schema.yml`-Dateien der Modelle (`dbt/models/staging/schema.yml`, `curated/schema.yml`, `trusted/schema.yml`, `sources.yml`) — jede Spalte trägt bereits im dbt-Projekt eine fachliche Beschreibung, z. B. bei `curated_esg_emissions`: `scope_1_tco2e` → "Scope 1 Emissionen in tCO2e (DECIMAL 18.3)", oder bei `trusted_esg_emissions` explizit der Hinweis "in Trusted garantiert nicht null". Diese Beschreibungen landen 1:1 als Column-Docs in OpenMetadata — **aber nur, wenn `catalog.json` vorhanden ist** (siehe Punkt 4 im Ingestion-Ablauf).
- **Testergebnisse**: aus `run_results.json` — welche dbt-Tests (`not_null`, `unique`, `accepted_values`, `relationships`, `dbt_utils.unique_combination_of_columns`, ...) auf welcher Spalte/Tabelle liefen und ob sie grün oder rot waren. Das ist besonders anschaulich am Kontrast staging → curated → trusted: die gleiche Spalte (`scope_1_tco2e`) hat in `curated` **keinen** `not_null`-Test, in `trusted` **explizit** einen — dokumentiert direkt in der jeweiligen `schema.yml`-Beschreibung ("in Trusted garantiert nicht null und im plausiblen Bereich"). Dieser Unterschied macht in OpenMetadata sichtbar, dass `trusted` fachlich strengere Garantien hat als `curated`.
- **Modellbeschreibungen** auf Tabellenebene, z. B. bei `trusted_esg_emissions`: "fachlich freigegebenes Endprodukt der Pipeline. Promotion aus Curated nur nach grünem Quality Gate (Great Expectations)." — das erklärt in der UI direkt, warum diese Tabelle existiert und was ihr Vertrauens-Status bedeutet, ohne dass man den Code lesen muss.
- **Source-Provenance-Dokumentation**: `sources.yml` beschreibt bereits die Raw-Layer-Spalten (`ingestion_id`, `source_file_hash`, `raw_payload`, ...) — dieselbe File-Level-Provenance, die im Code implementiert ist, taucht dann auch im Katalog als lesbare Spaltenbeschreibung auf.

Kurz gesagt: dbt liefert die **fachliche Semantik** (was bedeutet diese Spalte, welche Garantien gelten, wie hängen Tabellen zusammen), Trino/Nessie liefern nur die **technische Struktur** (welche Tabellen/Spalten/Typen existieren). Erst die Kombination beider Ingestion-Schritte macht den Katalog wirklich nützlich.

---

## 5. Glossary und Data Contract

### 5.1 Data Contract — Status: noch nicht umgesetzt

Das komplette Repository wurde nach "Data Contract" durchsucht (Code, Configs, Doku) — **es gibt dazu aktuell keine Implementierung und keine Konfigurationsdateien im Repo**.

Mögliche Einordnung für die Präsentation:
- Dieser Punkt gehört vermutlich zum **Scope des DE2-Hackathon-Projekts** (laut Referenz auf `DE-2-Hackathon-Projekt.docx`), ist aber entweder noch für den eigentlichen Hackathon-Tag geplant oder in einem externen Dokument beschrieben, das nicht im Repo liegt.
- OpenMetadata selbst unterstützt Data Contracts als Feature ab neueren Versionen — das technische Fundament (Server + Elasticsearch + Trino-Service) steht also bereits, es fehlt nur die inhaltliche Befüllung.

Möglicher nächster Schritt: das externe Word-Dokument (`DE-2-Hackathon-Projekt.docx`) daraufhin prüfen, ob dort bereits Vorgaben stehen.

### 5.2 Glossary — CSV-Import-Template, praktisch gegen den laufenden Server verifiziert

Anders als Data Contracts ist das Glossar **nicht** zwingend über die UI zu pflegen. Alternativen: CSV-Bulk-Import, direkte REST-API-Calls (`/api/v1/glossaries`, `/api/v1/glossaryTerms`) oder das Python SDK aus dem `openmetadata-ingestion`-Package (demselben, das bereits im `om-ingest`-venv für die Trino-/dbt-Ingestion installiert ist).

Für den CSV-Weg liegen zwei Dateien im Repo:
- **[`config/openmetadata/glossary-esg-terms.csv`](../config/openmetadata/glossary-esg-terms.csv)** — Ziel-CSV mit vollständigen `relatedTerms`-Verknüpfungen.
- **[`config/openmetadata/glossary-esg-terms-pass1-base.csv`](../config/openmetadata/glossary-esg-terms-pass1-base.csv)** — dieselben Terms ohne `relatedTerms`, siehe Fund 3 unten.

Die Spaltenreihenfolge (`parent`, `name*`, `displayName`, `description`, `synonyms`, `relatedTerms`, `references`, `tags`, `reviewers`, `owner`, `glossaryStatus`, `extension`) wurde gegen den OpenMetadata-Quellcode des im Repo gepinnten Tags `1.5.11-release` verifiziert (`GlossaryRepository.java` + `glossaryCsvDocumentation.json`) — neuere Releases haben zusätzliche Spalten (`color`, `iconURL`, `domains`), die es in `1.5.11` noch nicht gibt.

Inhaltlich befüllt mit drei Themen-Clustern passend zu den bereits in `dbt/models/*/schema.yml` dokumentierten Spalten: **Stammdaten** (ISIN, LEI, Sitzland), **ESG-Kennzahlen** (Scope 1/2/3) und **Datenqualitätsstatus** (Curated vs. Trusted).

**Der Import wurde nicht nur per Dry-Run getestet, sondern am 2026-07-24 tatsächlich produktiv durchgeführt** — das bereits bestehende, manuell über die UI gepflegte Glossar `MiniLakehouseFachbegriffe` (angelegt 2026-07-23 von n.hohmann, 8 flache Terms ohne Hierarchie) wurde exportiert, gesichert, gelöscht und mit der neuen hierarchischen Struktur neu aufgebaut. Dabei kamen vier reale Fehler/Abweichungen zum Vorschein, die die Doku allein nicht verraten hätte:

1. **Pflichtfeld-Markierung im Header ist literal Teil des Headers.** Der Server vergleicht den CSV-Header-String exakt gegen `[parent,name*,displayName,...]` — die Spalte muss buchstäblich `name*` heißen (mit Sternchen), nicht nur `name`. Mit `name` bricht der Import sofort mit `#INVALID_HEADER` ab, noch bevor eine einzige Zeile verarbeitet wird.
2. **`parent` muss der volle, glossar-qualifizierte Pfad sein**, nicht nur der Name des Eltern-Terms. Aus `Stammdaten` als `parent`-Wert wird `#INVALID_FIELD: Entity Stammdaten of type glossaryTerm not found` — korrekt ist `<Glossarname>.Stammdaten`. Dasselbe gilt für `relatedTerms`. Deshalb enthält das Template den Platzhalter `{{GLOSSARY}}`, der vor dem Import durch den tatsächlichen Zielglossar-Namen ersetzt werden muss (z. B. per `sed`).
3. **`relatedTerms`, die auf einen Geschwister-Term im selben Import verweisen, scheitern dauerhaft, auch bei wiederholtem Import derselben Datei** — verifiziert an einem zirkulären Fall (ISIN ↔ LEI, Curated ↔ Trusted): keine der beiden Seiten wird je angelegt, weil jede die andere als Voraussetzung braucht, die aber selbst nie entsteht. Der einzige stabile Ablauf ist **zweistufig mit zwei unterschiedlichen Dateien**, Pass 1 ohne, Pass 2 mit `relatedTerms` (siehe unten).
4. **`glossaryStatus` aus der CSV wird beim Anlegen ignoriert.** Zwei Terms im Template (`Sitzland`, `Scope 3 Emissionen`) waren explizit auf `Draft` gesetzt, landeten nach dem Import aber trotzdem auf `Approved` — verifiziert per direktem `GET` auf den Term danach. Kein Blocker, aber wer Draft-Status wirklich braucht, muss ihn aktuell manuell nachziehen (UI oder `PATCH`).

**Tatsächlich ausgeführter Ablauf** (nicht nur Empfehlung — das ist exakt das, was auf der laufenden Instanz passiert ist):

1. Bestehendes Glossar exportiert und als Backup abgelegt: [`config/openmetadata/backups/MiniLakehouseFachbegriffe-backup-20260724-083723.csv`](../config/openmetadata/backups/MiniLakehouseFachbegriffe-backup-20260724-083723.csv). Dabei fiel eine fünfte Abweichung auf: Das Owner-Feld wird im Export als `user:n.hohmann` (Doppelpunkt) geschrieben, während `glossaryCsvDocumentation.json` als Beispiel `user;john` (Semikolon) nennt — für `1.5.11` ist der Doppelpunkt das tatsächlich funktionierende Format, per Re-Import-Dry-Run des eigenen Exports bestätigt.
2. Die beiden Terms ohne Entsprechung im neuen Template (`NAV`, `SourceSystem`) wurden unverändert samt Owner-Attribution in beide CSV-Dateien übernommen, damit beim Neuaufbau nichts verloren geht.
3. Glossar gelöscht: `DELETE /api/v1/glossaries/name/MiniLakehouseFachbegriffe?recursive=true&hardDelete=true`.
4. Glossar mit identischem Namen, identischer Beschreibung und identischem Owner (`n.hohmann`) neu angelegt.
5. Zweistufiger Import — Pass 1 (`glossary-esg-terms-pass1-base.csv`, ohne `relatedTerms`) → **15/15 `success`**; Pass 2 (`glossary-esg-terms.csv`, mit `relatedTerms`) → **15/15 `success`** (`Entity updated`).

Ergebnis: **14 Terms**, hierarchisch unter `Stammdaten` (ISIN, LEI, Sitzland), `ESG-Kennzahlen` (Scope 1/2/2/3) und `Datenqualitaetsstatus` (Curated, Trusted), plus `NAV` und `SourceSystem` weiterhin auf Root-Ebene, alle `relatedTerms`-Verknüpfungen aufgelöst (inkl. der vom Server automatisch ergänzten Rückrichtung — z. B. verweist `Scope 1` jetzt auch auf `Scope 3`, weil `Scope 3` umgekehrt auf `Scope 1` verweist; die Relation ist laut Server-Modell symmetrisch).

Reproduzierbarer Ablauf für ein neues/anderes Zielglossar:

```bash
GLOSSARY=MiniLakehouseFachbegriffe   # Platzhalter durch echten Zielnamen ersetzen

sed "s/{{GLOSSARY}}/${GLOSSARY}/g" config/openmetadata/glossary-esg-terms-pass1-base.csv > /tmp/pass1.csv
sed "s/{{GLOSSARY}}/${GLOSSARY}/g" config/openmetadata/glossary-esg-terms.csv           > /tmp/pass2.csv

curl -X PUT "http://openmetadata-server:8585/api/v1/glossaries/name/${GLOSSARY}/import?dryRun=false" \
  -H "Authorization: Bearer ${OPENMETADATA_JWT_TOKEN}" -H "Content-Type: text/plain" --data-binary @/tmp/pass1.csv

curl -X PUT "http://openmetadata-server:8585/api/v1/glossaries/name/${GLOSSARY}/import?dryRun=false" \
  -H "Authorization: Bearer ${OPENMETADATA_JWT_TOKEN}" -H "Content-Type: text/plain" --data-binary @/tmp/pass2.csv
```

`dryRun=true` statt `dryRun=false` validiert vorab, ohne zu schreiben.

### 5.3 Nachspiel: Hard-Delete eines Glossars reißt Spalten-Verknüpfungen mit — Incident & Recovery

Nach dem Neuaufbau fiel auf, dass Tabellen wie `curated_companies` plötzlich keine "Glossary Terms" mehr auf ihren Spalten zeigten. **Ursache: Das `hardDelete=true` beim Löschen eines Glossars entfernt nicht nur die Term-Entities selbst, sondern reißt still und ohne Versions-Eintrag auch jede Verknüpfung mit, die dieser Term zuvor als Tag an einer Tabellen-/Spalten-Entität hatte** — das Export/Backup vor dem Löschen (Abschnitt 5.2) hatte nur die Term-**Definitionen** gesichert, nicht diese Verknüpfungen. Das war eine Lücke im ursprünglichen Vorgehen.

**Wiederherstellung über die OpenMetadata-Versionshistorie** (jede Tabellen-Entität führt ihre eigene Change-Historie unabhängig vom Glossar):

1. Alle 14 Tabellen im Service `trino_lakehouse` durchsucht (`GET /api/v1/tables/{id}/versions`), `changeDescription.fieldsAdded`/`fieldsUpdated` nach `columns.*.tags`-Feldern mit `MiniLakehouseFachbegriffe`-Tags gefiltert. Ergebnis: **16 verlorene Spalten-Verknüpfungen über 4 Tabellen** (`curated_companies`, `curated_esg_emissions`, `snp_fondspreise_scd2`, `trusted_esg_emissions`) — nicht nur die eine, die zuerst auffiel.
2. Alte Term-Namen auf die neuen (umbenannten/verschachtelten) FQNs gemappt, z. B. `MiniLakehouseFachbegriffe.Scope1Emissionen` → `MiniLakehouseFachbegriffe.ESG-Kennzahlen.Scope 1 Emissionen`.
3. Alle 16 Verknüpfungen per `PATCH` auf die jeweilige Spalte neu gesetzt.

**Dabei zweiter, selbst verursachter Fehler beim Reparieren:** Ein `PATCH`-Aufruf mit `{"op": "add", "path": "/columns/{idx}/tags/-", "value": {...}}` (klassisches JSON-Patch-Array-Append) hat bei `curated_companies.isin` nicht angehängt, sondern den bestehenden Klassifikations-Tag `PII.NonSensitive` **ersetzt** — verifiziert über dieselbe Versionshistorie-Technik (die Änderung zeigte sich als `ADDED` + `DELETED` im selben Versionssprung). Zusätzlich verursachte ein Korrekturversuch ohne den Query-Parameter `?fields=tags,columns` beim vorherigen `GET` einen zweiten, ähnlichen Fehler (Spalten-Tags kommen ohne diesen Parameter leer zurück, nicht weil sie leer sind). Endgültig behoben durch: Tabelle **mit** `fields=tags,columns` frisch abrufen, gewünschten Ziel-Tag-Satz explizit berechnen, per `replace` (nicht `add .../-`) auf den kompletten Spalten-Tags-Array schreiben, und das Ergebnis mit einem unabhängigen erneuten `GET` verifizieren statt der PATCH-Response zu vertrauen.

**Abschließend für alle 16 Verknüpfungen gegen die rekonstruierte Soll-Liste verifiziert — alles korrekt, inklusive des wiederhergestellten `PII.NonSensitive`-Tags.**

Zwei Lektionen für die Präsentation:
- **Ein Glossar-Backup muss auch erfassen, wo die Terms als Tags verwendet werden**, nicht nur die Term-Definitionen selbst — sonst verliert ein Hard-Delete stillschweigend Verknüpfungen, die nirgends im Glossar-Export sichtbar sind.
- **JSON-Patch-Array-Append (`path/-`) auf das `tags`-Feld von Spalten verhält sich in dieser OpenMetadata-Version nicht wie erwartet** — zuverlässig ist nur: aktuellen Zustand mit vollständigen `fields` abfragen, gewünschten kompletten Array-Inhalt clientseitig zusammenbauen, per `replace` schreiben, unabhängig verifizieren.

---

## Quellen im Repo

- `docker-compose.openmetadata.yml`
- `scripts/init-openmetadata-db.sh`
- `config/openmetadata/ingest-trino.yaml`
- `config/openmetadata/ingest-dbt.yaml`
- `docs/OPENMETADATA-SETUP.md`
- `dbt/models/sources.yml`, `dbt/models/staging/schema.yml`, `dbt/models/curated/schema.yml`, `dbt/models/trusted/schema.yml`
- `config/openmetadata/glossary-esg-terms.csv`, `config/openmetadata/glossary-esg-terms-pass1-base.csv`
- `config/openmetadata/backups/MiniLakehouseFachbegriffe-backup-20260724-083723.csv`
- Live-Migration gegen den laufenden `openmetadata-server`: Export, Löschung, Neuaufbau des Glossars `MiniLakehouseFachbegriffe` (2026-07-24)
