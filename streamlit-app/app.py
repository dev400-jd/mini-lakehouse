"""
ESG-Dashboard — anschauliche Präsentationsschicht über dem Lakehouse.

Zieht live zusammen:
- Trino  -> Fonds-Features aus dem Iceberg-raw-Layer
- FastAPI (model-api) -> Cluster-Vorhersage aus dem registrierten Modell
- MLflow (REST) -> Silhouette je k (Modellvergleich)
"""
import os

import pandas as pd
import requests
import streamlit as st
from trino.dbapi import connect

FASTAPI_URL = os.environ.get("FASTAPI_URL", "http://model-api:8000")
MLFLOW_URI = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))
EXPERIMENT = "fonds-co2-fussabdruck"
FEATURES = ["n_holdings", "w_scope1", "w_scope2", "pct_energy", "pct_materials"]

FEATURE_SQL = """
WITH latest AS (SELECT max(position_date) AS d FROM raw.fund_positions),
     emis AS (
        SELECT isin, sector, scope_1_tco2e, scope_2_market_tco2e
        FROM raw.nzdpu_emissions WHERE reporting_year = 2023
     )
SELECT m.fund_name, m.fund_type,
       count(*)                                            AS n_holdings,
       sum(p.weight_pct / 100.0 * e.scope_1_tco2e)         AS w_scope1,
       sum(p.weight_pct / 100.0 * e.scope_2_market_tco2e)  AS w_scope2,
       sum(p.weight_pct) FILTER (WHERE e.sector = 'Energy')    AS pct_energy,
       sum(p.weight_pct) FILTER (WHERE e.sector = 'Materials') AS pct_materials
FROM raw.fund_positions p
JOIN latest l ON p.position_date = l.d
JOIN emis e   ON p.holding_isin = e.isin
JOIN raw.fund_master m ON p.fund_isin = m.fund_isin
GROUP BY m.fund_name, m.fund_type
ORDER BY w_scope1 DESC
"""

st.set_page_config(page_title="Fonds-CO₂ ESG-Dashboard", page_icon="🌍", layout="wide")


@st.cache_data(ttl=60)
def load_funds():
    conn = connect(host=TRINO_HOST, port=TRINO_PORT, user="streamlit",
                   catalog="nessie", schema="raw")
    df = pd.read_sql(FEATURE_SQL, conn)
    df[["pct_energy", "pct_materials"]] = df[["pct_energy", "pct_materials"]].fillna(0.0)
    return df


@st.cache_data(ttl=60)
def load_silhouette():
    exp = requests.get(f"{MLFLOW_URI}/api/2.0/mlflow/experiments/get-by-name",
                       params={"experiment_name": EXPERIMENT}, timeout=10).json()
    eid = exp["experiment"]["experiment_id"]
    runs = requests.post(f"{MLFLOW_URI}/api/2.0/mlflow/runs/search",
                         json={"experiment_ids": [eid], "max_results": 100}, timeout=10).json()
    rows = []
    for r in runs.get("runs", []):
        params = {p["key"]: p["value"] for p in r["data"].get("params", [])}
        metrics = {m["key"]: m["value"] for m in r["data"].get("metrics", [])}
        tags = {t["key"]: t["value"] for t in r["data"].get("tags", [])}
        if tags.get("mlflow.runName", "").startswith("kmeans_k") and "silhouette" in metrics:
            rows.append({"k": int(params["k"]), "silhouette": metrics["silhouette"]})
    return pd.DataFrame(rows).drop_duplicates("k").sort_values("k")


def predict(payload):
    r = requests.post(f"{FASTAPI_URL}/predict", json=payload, timeout=10)
    r.raise_for_status()
    return r.json()["cluster"]


# ---------------------------------------------------------------- Header
st.title("🌍 Fonds-CO₂-Fußabdruck — ESG-Dashboard")
st.caption("Live aus dem Lakehouse:  Trino (Features)  ·  MLflow (Metriken)  ·  FastAPI (Modell)")

try:
    h = requests.get(f"{FASTAPI_URL}/health", timeout=5).json()
    if h.get("model_loaded"):
        st.success(f"✅ Model-API verbunden · Modell: `{h.get('model_uri', '')}`")
    else:
        st.warning("⚠️ Model-API erreichbar, aber kein Modell geladen.")
except Exception as e:  # noqa: BLE001
    st.error(f"❌ Model-API nicht erreichbar ({FASTAPI_URL}): {e}")

# ---------------------------------------------------------------- Daten
funds = None
try:
    funds = load_funds()
    funds["cluster"] = funds.apply(
        lambda r: predict({c: (int(r[c]) if c == "n_holdings" else float(r[c])) for c in FEATURES}),
        axis=1,
    )
    funds["Cluster"] = funds["cluster"].map({0: "0 · CO₂-intensiver", 1: "1 · emissionsärmer"}).fillna(
        funds["cluster"].astype(str))
except Exception as e:  # noqa: BLE001
    st.error(f"Fonds-Daten/Vorhersage nicht verfügbar: {e}")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Modellvergleich — Silhouette je k")
    try:
        sdf = load_silhouette().set_index("k")
        st.bar_chart(sdf, y="silhouette", height=300)
        best = int(sdf["silhouette"].idxmax())
        st.caption(f"Optimum bei **k = {best}** (Silhouette {sdf['silhouette'].max():.3f}) — mehr Cluster ist nicht besser.")
    except Exception as e:  # noqa: BLE001
        st.info(f"Keine MLflow-Metriken verfügbar: {e}")

with col2:
    st.subheader("Cluster-Verteilung der Fonds")
    if funds is not None:
        st.scatter_chart(funds, x="w_scope1", y="pct_energy", color="Cluster",
                         size="n_holdings", height=300)
        st.caption("x: gewichtete Scope-1-Emissionen · y: Energy-Exposure (%) · Größe: Anzahl Positionen")

# ---------------------------------------------------------------- Tabelle
if funds is not None:
    st.subheader("Fonds-Übersicht — Live-Vorhersage über FastAPI")
    st.dataframe(
        funds[["fund_name", "fund_type", "n_holdings", "w_scope1", "w_scope2",
               "pct_energy", "pct_materials", "Cluster"]],
        use_container_width=True, hide_index=True,
        column_config={
            "w_scope1": st.column_config.NumberColumn("w_scope1", format="%.0f"),
            "w_scope2": st.column_config.NumberColumn("w_scope2", format="%.0f"),
            "pct_energy": st.column_config.NumberColumn("Energy %", format="%.1f"),
            "pct_materials": st.column_config.NumberColumn("Materials %", format="%.1f"),
        },
    )

# ---------------------------------------------------------------- Rechner
st.subheader("🔮 Interaktiver Fonds-Rechner")
st.caption("Feature-Werte eingeben → Cluster-Vorhersage über den FastAPI-Endpoint.")
with st.form("predict_form"):
    c = st.columns(5)
    n = c[0].number_input("n_holdings", 1, 100, 15)
    s1 = c[1].number_input("w_scope1", 0.0, 5e7, 7_000_000.0, step=100_000.0)
    s2 = c[2].number_input("w_scope2", 0.0, 5e6, 350_000.0, step=10_000.0)
    pe = c[3].number_input("pct_energy", 0.0, 100.0, 12.0)
    pm = c[4].number_input("pct_materials", 0.0, 100.0, 6.0)
    submitted = st.form_submit_button("Cluster vorhersagen", type="primary")
if submitted:
    try:
        cluster = predict({"n_holdings": int(n), "w_scope1": s1, "w_scope2": s2,
                           "pct_energy": pe, "pct_materials": pm})
        label = {0: "CO₂-intensiver", 1: "emissionsärmer"}.get(cluster, "—")
        st.metric("Vorhergesagtes Cluster", f"{cluster}  ·  {label}")
    except Exception as e:  # noqa: BLE001
        st.error(f"Vorhersage fehlgeschlagen: {e}")
