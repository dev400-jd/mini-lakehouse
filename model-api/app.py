import os

import mlflow
import mlflow.sklearn
import pandas as pd
from fastapi import FastAPI
from pydantic import BaseModel

# Registriertes Modell (Pipeline: StandardScaler + KMeans) — nimmt Roh-Features.
MODEL_URI = os.environ.get("MODEL_URI", "models:/fonds-esg-clustering/latest")
mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"))

FEATURE_ORDER = ["n_holdings", "w_scope1", "w_scope2", "pct_energy", "pct_materials"]

app = FastAPI(title="Fonds-ESG Model API")
_model = None
_load_error = None


def _load():
    """Modell aus der MLflow-Registry laden (mit ein paar Retries, falls
    der Tracking-Server beim Start noch nicht bereit ist)."""
    global _model, _load_error
    import time
    for attempt in range(5):
        try:
            _model = mlflow.sklearn.load_model(MODEL_URI)
            _load_error = None
            return
        except Exception as e:  # noqa: BLE001
            _load_error = str(e)
            time.sleep(3)


@app.on_event("startup")
def startup():
    _load()


class Features(BaseModel):
    n_holdings: int
    w_scope1: float
    w_scope2: float
    pct_energy: float
    pct_materials: float


@app.get("/")
def root():
    return {"message": "Model API läuft", "model_loaded": _model is not None}


@app.get("/health")
def health():
    if _model is None:
        return {"status": "degraded", "model_loaded": False, "error": _load_error}
    return {"status": "ok", "model_loaded": True, "model_uri": MODEL_URI}


@app.post("/reload")
def reload_model():
    """Modell erneut aus der Registry laden (z. B. nach einer neuen Promotion)."""
    _load()
    return health()


@app.post("/predict")
def predict(f: Features):
    if _model is None:
        return {"error": "model not loaded", "detail": _load_error}
    X = pd.DataFrame([[getattr(f, c) for c in FEATURE_ORDER]], columns=FEATURE_ORDER)
    cluster = int(_model.predict(X)[0])
    return {"cluster": cluster, "features": f.dict()}
