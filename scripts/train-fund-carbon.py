#!/usr/bin/env python
# =============================================================================
# train-fund-carbon.py — Fonds-CO2-Fussabdruck + ESG-Clustering mit MLflow
#
# Pipeline:
#   1. Feature-Extraktion aus dem Lakehouse (Trino -> Iceberg raw layer)
#      Pro Fonds: gewichtete Scope-1/2-Emissionen der Holdings + Sektor-Exposure
#   2. KMeans-Clustering mit k-Sweep (2..6), Standardisierung
#   3. MLflow-Tracking: pro k ein Run (Params, Silhouette, Inertia, Modell, Plot)
#   4. Bestes k (max. Silhouette) -> Model Registry + gelabelte Fondsliste
#
# Ausfuehrung im jupyter-Container (hat mlflow-Client, sklearn, trino, MLFLOW_TRACKING_URI):
#   docker exec jupyter python /tmp/train-fund-carbon.py
# =============================================================================

import os
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
import mlflow
import mlflow.sklearn
from mlflow.models.signature import infer_signature
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from trino.dbapi import connect

EXPERIMENT = "fonds-co2-fussabdruck"
REGISTERED_MODEL = "fonds-esg-clustering"
EMISSIONS_YEAR = 2023

# --- 1. Feature-Extraktion aus dem Lakehouse -------------------------------
# Pro Fonds (letzter Stichtag): gewichtete Emissionen + Exposure in
# CO2-intensiven Sektoren. weight_pct summiert je Fonds auf 100.
FEATURE_SQL = f"""
WITH latest AS (SELECT max(position_date) AS d FROM raw.fund_positions),
     emis AS (
        SELECT isin, sector, scope_1_tco2e, scope_2_market_tco2e
        FROM raw.nzdpu_emissions
        WHERE reporting_year = {EMISSIONS_YEAR}
     )
SELECT
    m.fund_name,
    m.fund_type,
    count(*)                                                          AS n_holdings,
    sum(p.weight_pct / 100.0 * e.scope_1_tco2e)                       AS w_scope1,
    sum(p.weight_pct / 100.0 * e.scope_2_market_tco2e)               AS w_scope2,
    sum(p.weight_pct) FILTER (WHERE e.sector = 'Energy')             AS pct_energy,
    sum(p.weight_pct) FILTER (WHERE e.sector = 'Materials')          AS pct_materials
FROM raw.fund_positions p
JOIN latest l   ON p.position_date = l.d
JOIN emis e     ON p.holding_isin = e.isin
JOIN raw.fund_master m ON p.fund_isin = m.fund_isin
GROUP BY m.fund_name, m.fund_type
ORDER BY w_scope1 DESC
"""

FEATURES = ["n_holdings", "w_scope1", "w_scope2", "pct_energy", "pct_materials"]


def load_features():
    conn = connect(host="trino", port=8080, user="mlflow", catalog="nessie", schema="raw")
    df = pd.read_sql(FEATURE_SQL, conn)
    # Fonds ohne Energy/Materials-Holdings -> 0 statt NULL
    df[["pct_energy", "pct_materials"]] = df[["pct_energy", "pct_materials"]].fillna(0.0)
    return df


def main():
    mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))
    mlflow.set_experiment(EXPERIMENT)

    df = load_features()
    print(f"Features geladen: {len(df)} Fonds, {len(FEATURES)} Features")

    X = df[FEATURES].to_numpy(dtype="float64")
    scaler = StandardScaler().fit(X)
    X_scaled = scaler.transform(X)

    results = []
    for k in range(2, 7):
        with mlflow.start_run(run_name=f"kmeans_k{k}"):
            km = KMeans(n_clusters=k, random_state=42, n_init=10)
            labels = km.fit_predict(X_scaled)
            sil = silhouette_score(X_scaled, labels)

            mlflow.log_param("algorithm", "KMeans")
            mlflow.log_param("k", k)
            mlflow.log_param("features", ",".join(FEATURES))
            mlflow.log_param("emissions_year", EMISSIONS_YEAR)
            mlflow.log_metric("silhouette", sil)
            mlflow.log_metric("inertia", km.inertia_)

            # Scaler + KMeans als Pipeline loggen -> self-contained, nimmt Roh-Features
            pipe = Pipeline([("scaler", scaler), ("kmeans", km)])
            signature = infer_signature(X, labels)
            mlflow.sklearn.log_model(pipe, artifact_path="model", signature=signature)

            # Cluster-Scatter als Artefakt
            fig, ax = plt.subplots(figsize=(7, 5))
            sc = ax.scatter(df["w_scope1"], df["pct_energy"], c=labels, cmap="viridis", s=120)
            for _, row in df.iterrows():
                ax.annotate(row["fund_name"][:18], (row["w_scope1"], row["pct_energy"]),
                            fontsize=7, alpha=0.7)
            ax.set_xlabel("Gewichtete Scope-1-Emissionen")
            ax.set_ylabel("Energy-Exposure (%)")
            ax.set_title(f"Fonds-Cluster (k={k}, Silhouette={sil:.3f})")
            fig.colorbar(sc, label="Cluster")
            fig.tight_layout()
            plot_path = f"/tmp/clusters_k{k}.png"
            fig.savefig(plot_path, dpi=110)
            plt.close(fig)
            mlflow.log_artifact(plot_path, artifact_path="plots")

            run_id = mlflow.active_run().info.run_id
            results.append((k, sil, run_id))
            print(f"  k={k}: silhouette={sil:.3f}, inertia={km.inertia_:.1f}")

    # --- bestes k -> Registry + gelabelte Fondsliste -----------------------
    best_k, best_sil, best_run = max(results, key=lambda r: r[1])
    print(f"\nBestes Modell: k={best_k} (silhouette={best_sil:.3f})")

    model_uri = f"runs:/{best_run}/model"
    mlflow.register_model(model_uri, REGISTERED_MODEL)

    # Finaler, gelabelter Report als eigener Run
    with mlflow.start_run(run_name=f"final_labeled_k{best_k}"):
        km = KMeans(n_clusters=best_k, random_state=42, n_init=10)
        df["cluster"] = km.fit_predict(X_scaled)
        mlflow.log_param("k", best_k)
        mlflow.log_metric("silhouette", best_sil)
        report_path = "/tmp/fonds_co2_report.csv"
        df.to_csv(report_path, index=False)
        mlflow.log_artifact(report_path, artifact_path="report")
        print("\nGelabelter Fonds-Report:")
        print(df[["fund_name", "fund_type", "w_scope1", "pct_energy", "cluster"]].to_string(index=False))


if __name__ == "__main__":
    main()
