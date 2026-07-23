# DS2 Findings: Der souveräne Lakehouse-Agent

Hackathon: Data-&-Analytics-Hackathon, Deka Investment. Gruppe DS2 (Martin, Rupert).
Stand: 2026-07-23.

## 1. Ziel

Kann ein vollstaendig lokaler, Open-Source-Agent (Trino MCP-Server + Ollama-Modell,
verbunden ueber eine Bridge) natuerlichsprachliche Fragen gegen das Mini-Lakehouse
beantworten — und wie zuverlaessig? Der Data-Science-Kern der Aufgabe ist nicht der
Aufbau der Kette (Konfiguration fertiger Open-Source-Komponenten), sondern ihre
systematische Evaluation.

## 2. Aufbau

| Glied | Komponente | Konfiguration |
|---|---|---|
| MCP-Server | [`tuannvm/mcp-trino`](https://github.com/tuannvm/mcp-trino) v4.3.1 | read-only (Default, kein `TRINO_ALLOW_WRITE_QUERIES`), `TRINO_MAX_ROWS=1000`, `TRINO_QUERY_TIMEOUT=30`, `TRINO_SCHEME=http`/`TRINO_SSL=false` (Server nimmt sonst HTTPS an) |
| Sprachmodell | Ollama, `qwen2.5:7b` (4.7 GB) und `qwen2.5:1.5b` (986 MB) | zwei Groessenstufen derselben Modellfamilie fuer den Vergleich |
| Bridge (interaktiv) | [`ollmcp`](https://github.com/jonigl/mcp-client-for-ollama) v0.33.1 | Human-in-the-Loop-Bestaetigung per Default aktiv |
| Bridge (Eval) | eigener, minimaler Harness (`mcp_agent/`) | siehe Abschnitt 3 |

Sicherheitsleitplanken verifiziert (nicht nur behauptet): ein `DROP TABLE`-Versuch ueber
den MCP-Server wird serverseitig mit `security restriction: only SELECT, SHOW, DESCRIBE,
and EXPLAIN queries are allowed` abgelehnt — unabhaengig davon, was das Modell versucht.

## 3. Warum ein eigener Eval-Harness statt `ollmcp` direkt zu automatisieren

`ollmcp` ist eine interaktive TUI mit einem live "press 'a' to abort"-Tastatur-Listener.
Automatisierung ueber Pipes/PTY (`printf ... | ollmcp`, auch mit `script -q /dev/null`)
fuehrte zu Race Conditions zwischen diesem Listener und den gescripteten Eingaben (Fragen
wurden sofort "aborted", oder das Terminal-Rendering wiederholte sich endlos). Fuer
reproduzierbare, batch-faehige Auswertung ueber 18 Fragen x 2 Modelle x 3 Wiederholungen
war das nicht praktikabel.

Der Harness (`mcp_agent/eval_harness.py`, `mcp_agent/mcp_stdio_client.py`) umgeht die TUI
komplett und spricht MCP-Server und Ollama direkt an:
- **Kein neuer Dependency**: der MCP-Client ist handgeschrieben (~90 Zeilen) statt das
  offizielle `mcp`-SDK zu installieren. Das Protokoll wurde vorab per Hand-Probe verifiziert:
  `mcp-trino` spricht ueber stdio schlichtes zeilenweises JSON-RPC 2.0, ohne
  Content-Length-Framing — trivial zu implementieren.
- Ollama wird ueber die REST-API (`/api/chat`, `requests`, bereits Projekt-Dependency)
  angesprochen, inklusive `tools`-Schema (aus den MCP-Tool-Definitionen abgeleitet).
- Automatisierte Ausfuehrung ist hier sicher: die Fragen sind ausschliesslich lesend, und
  der MCP-Server erzwingt Read-Only ohnehin serverseitig — Human-in-the-Loop ist eine
  Absicherung fuer interaktive/produktive Nutzung, nicht fuer einen isolierten
  Sandbox-Eval-Lauf.

## 4. Fragenkatalog

18 Fragen in `mcp_agent/questions.json`, mit Ground Truth vorab per direktem SQL gegen
Trino ermittelt (nicht ueber den Agenten). Kategorien: discovery (3), schema (1),
aggregation (7), topn (2), join_topn/join_compare (2), data_quality (1), negative (1),
distractor (1).

Zwei Fragen verdienen besondere Erwaehnung:
- **Q17 (negative)**: "Welche Tabellen gibt es im 'gold' Schema?" — es gibt kein Schema
  `gold`. Testet, ob das Modell das explizit sagt statt Tabellen zu erfinden.
- **Q18 (distractor)**: vergleicht die echte dbt-Tabelle `staging.stg_nzdpu_emissions`
  (90 Zeilen, Teil der Produktions-Pipeline) mit `staging.nzdpu_emissions_flat`
  (180 Zeilen, eine Demo-Tabelle aus `notebooks/02_time_travel_schema_evolution.ipynb`,
  kein Teil der echten Pipeline). Testet, ob das Modell aehnlich benannte Tabellen
  verwechselt.

## 5. Zwei Bugs im Harness selbst gefunden und behoben

Bevor die Zahlen belastbar waren, mussten zwei Probleme in der automatischen Bewertung
selbst behoben werden — beide sind fuer sich genommen eine Lektion ueber automatisierte
LLM-Evaluation:

**Bug 1 — "any" vs. "all" beim Substring-Check.** Q17s `check_strings` waren alternative
Formulierungen von "existiert nicht" (`nicht`, `kein`, `not exist`, ...). Der urspruengliche
Check verlangte, dass ALLE davon im Antworttext vorkommen — beide Modelle beantworteten
Q17 tatsaechlich korrekt, wurden aber nur als `partial` bewertet. Fix: neues Feld
`check_mode` pro Frage (`"all"` Default, `"any"` fuer Q17).

**Bug 2 — Bewertung nur des Antworttexts, nicht der tatsaechlichen Tool-Ergebnisse.**
Bei Q14 (Bewertungsfrage: meldet `cdp` oder `nzdpu` den niedrigeren Wert — eine
Zwei-Optionen-Frage) bewertete der Harness `qwen2.5:1.5b` faelschlich mit `2/3 pass`,
obwohl das Modell in **keinem** der drei Laeufe ueberhaupt ein Tool aufgerufen hat. Es hat
stattdessen unzusammenhaengend ueber "BPs Kupferpreisindex" fabuliert und dabei zufaellig
das Wort "nzdpu" erwaehnt, was den Substring-Check faelschlich triggerte. Fix: ein
zusaetzlicher `grounded_check`, der ausschliesslich den echten Tool-Ergebnis-Text aus dem
Trace prueft (was Trino tatsaechlich zurueckgegeben hat), unabhaengig vom Modelltext. Das
finale `auto_check`-Urteil kombiniert beides:
- `pass`: richtige Daten abgerufen UND korrekt im Antworttext wiedergegeben
- `no_evidence`: die erwarteten Werte tauchen nirgends in den echten Tool-Ergebnissen auf
  (Halluzination, unabhaengig davon was der Antworttext behauptet)
- `partial_mismatch`: die richtigen Daten wurden abgerufen, aber im finalen Antworttext
  falsch wiedergegeben

Nach dem Fix zeigt Q14 fuer `qwen2.5:1.5b` korrekt `0/3` mit `no_evidence 3/3`.

**Lektion fuer die Evaluations-Methodik**: naives Keyword-Matching gegen den Antworttext
eines Sprachmodells ist bei Ja/Nein- oder Zwei-Optionen-Fragen leicht durch Zufall
"bestehbar" — ein Agenten-Eval-Harness muss die tatsaechlichen Tool-Ergebnisse pruefen,
nicht nur die Modell-Prosa.

## 6. Ergebnisse (finaler, korrigierter Lauf)

18 Fragen x 2 Modelle x 3 unabhaengige Wiederholungen = 108 Laeufe.
Rohdaten: `mcp_agent/results/results_20260723T123505Z.{json,md}`.

| Modell | pass | no_evidence | partial_mismatch |
|---|---|---|---|
| `qwen2.5:7b` | 18/54 (33%) | 36/54 (67%) | 0 |
| `qwen2.5:1.5b` | 9/54 (17%) | 45/54 (83%) | 0 |

Wichtiger Methodik-Hinweis: ein einzelner Lauf pro Frage ist **nicht** aussagekraeftig.
Ollamas Default-Sampling ist nicht deterministisch/geseedet — zwei Laeufe derselben 18
Fragen auf demselben Modell ergaben unterschiedliche Pass/Fail-Verteilungen pro Frage
(z.B. `qwen2.5:1.5b` schwankte zwischen 1 und 3 Paesse auf demselben Fragenkatalog).
Deshalb `--repeats 3` und Pass-*Rate* statt Pass/Fail als Kennzahl.

### Klares, reproduzierbares Muster ueber alle 3 Wiederholungen

- **Beide Modelle: 3/3 auf Q1 (Schema-Liste), Q4 (Spalten-Inspektion), Q17 (negativ)** —
  einfache Metadaten-Abfragen und der Halluzinations-Vermeidungstest funktionieren
  zuverlaessig bei beiden Groessen.
- **`qwen2.5:7b` scheitert konsistent (0/3, alle drei Wiederholungen identisch) an jeder
  Frage, die "Layer" korrekt als Schema statt als Katalog aufloesen muss**: Q2, Q3, Q5-Q10,
  Q13, Q16. Das ist kein Rauschen — es ist ein systematischer blinder Fleck.
- `qwen2.5:7b` erreicht nur bei den komplexeren Join-/Aggregationsfragen (Q11, Q12, Q14,
  Q15) gelegentlich (1-2 von 3) ein korrektes Ergebnis, nie zuverlaessig.
- **`qwen2.5:1.5b` liegt bei 0/3 auf allem ausser Q1, Q4, Q17** — es gelingen im Wesentlichen
  nur die zwei leichtesten Lookups plus der Negativtest.
- **Q18 (Distraktor) trennt die Modellgroessen am schaerfsten**: `qwen2.5:7b` **3/3** korrekt
  (90 vs. 180 Zeilen, als Unterschied benannt), `qwen2.5:1.5b` **0/3** — und erfindet dabei
  sogar eine falsche Zeilenzahl ("109 Zeilen") plus eine frei erfundene technische
  Begruendung ("Flat View... mit reduzierter Wartbarkeit").

## 7. Konkrete Fehlerbeispiele (aus dem finalen Lauf)

**Katalog/Schema-Verwechslung (`qwen2.5:7b`, Q2)** — ruft `list_tables` mit
`catalog='nlik_trusted'` auf (kein solcher Katalog existiert; nur `nessie` und `system`):
```
Tool: list_tables {"catalog": "nlik_trusted"}
  -> ERROR: Catalog 'nlik_trusted' not found
Antwort: "Der Katalog 'nlik_trusted' wurde nicht gefunden. Es scheint, dass es sich bei
dem Namen moeglicherweise um einen Tippfehler handelt oder der Katalog noch nicht
erstellt wurde."
```
Bemerkenswert: dasselbe Modell hat in einer **live ollmcp-Session** dieselbe Frage
("Welche Tabellen gibt es im Trusted Layer?") korrekt geloest, indem es nach einem
Fehlversuch selbststaendig auf `list_catalogs` -> `list_schemas` umgeschwenkt ist. Im
Harness (anderer System-Prompt, Single-Shot-Skript statt mehrzuegiger interaktiver
Session) gibt das Modell nach dem ersten Fehler meist einfach auf, statt zu explorieren.
Das ist selbst ein Befund: Tool-Call-Zuverlaessigkeit haengt sichtbar vom Rahmen
(Systemprompt/Session-Setup) ab, nicht nur vom Modell.

**Erfundene Tabellen/Spalten (`qwen2.5:7b`, Q9)**:
```
Tool: execute_query {"query": "SELECT ticker, security_name, weight_pct, as_of_date,
fund_id FROM fund_holdings WHERE weight_pct = (SELECT MAX(weight_pct) FROM
fund_holdings)"}
  -> ERROR: Table 'nessie.trusted.fund_holdings' does not exist
```
Weder die Tabelle `fund_holdings` noch die Spalten `ticker`/`security_name`/`as_of_date`
existieren im Schema — vollstaendig erfunden, plausibel benannt.

**Zero-tool-call Halluzination (`qwen2.5:1.5b`, Q6)** — keine Tool-Aufrufe, stattdessen
eine Meta-Beschreibung dessen, was man tun *koennte*, ohne es zu tun; die finale Antwort
liefert keine Zahl.

**Selbstbewusste falsche Praezision (`qwen2.5:1.5b`, Q18)**:
```
"Die Tabelle `nessie.staging.stg_nzdpu_emissions` hat in der aktuellen Datenbankversion
109 Zeilen. Die Unterschiede sind: - Die Tabelle `stg_nzdpu_emissions_flat` ist eine Flat
View, die den gleichen Inhalt wie `stg_nzdpu_emissions` bietet, aber mit reduzierter
Wartbarkeit..."
```
Ground Truth ist 90 Zeilen (nicht 109); `nzdpu_emissions_flat` ist keine "View" und hat
nichts mit "Wartbarkeit" zu tun — beides frei erfunden, aber mit hoher Zuversicht
formuliert.

## 8. Grenzen dieser Auswertung

- 18 Fragen, 2 Modelle derselben Familie (Qwen2.5) — kein Cross-Family-Vergleich (z.B.
  gegen Llama), das waere ein naheliegender STRETCH-Ausbau.
- `auto_check` bleibt trotz `grounded_check`-Fix ein Substring-Heuristik-Verfahren, kein
  echtes semantisches Grading. Grenzfaelle (z.B. teilweise richtige Zahlen, korrekte Zahl
  aber falsche Einheit) koennen falsch klassifiziert sein — bei Unsicherheit immer
  `final_answer` gegen `ground_truth` von Hand pruefen.
- Kein Vergleich unterschiedlicher Tool-Beschreibungen oder reduzierter Tool-Mengen (das
  waere das STRETCH-Interventions-Experiment aus dem Briefing) — naheliegender naechster
  Schritt: die Tool-Beschreibungen von `list_catalogs`/`list_schemas`/`list_tables` um einen
  Hinweis auf die konkrete Katalog/Schema-Konvention dieses Lakehouse ergaenzen (z.B. "In
  diesem Lakehouse ist der Katalog immer 'nessie'; Layer wie raw/staging/curated/trusted
  sind Schemas, keine Kataloge.") und denselben Fragenkatalog erneut messen — das ist
  exakt der Fehlermodus, der in Abschnitt 6/7 dominiert.

## 9. Reproduzieren

```bash
cd mcp_agent
uv run --project .. python eval_harness.py --models qwen2.5:7b qwen2.5:1.5b --repeats 3
```
Ergebnisse landen in `mcp_agent/results/results_<timestamp>.{json,md}`. `--limit N` fuer
einen schnellen Testlauf mit nur den ersten N Fragen, `--models <name>` fuer ein einzelnes
Modell.
