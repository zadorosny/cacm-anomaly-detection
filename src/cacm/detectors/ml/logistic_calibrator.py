"""M3 — Calibrador logístico (supervisionado leve).

Treinado sobre alertas históricos rotulados (CONFIRMADO vs FALSO_POSITIVO).
Recalibra o score combinado: oferece um meta-score que reflete probabilidade
de ser confirmado pelo auditor (feedback loop CA/CM).
"""

from __future__ import annotations

from pathlib import Path

import joblib
import mlflow
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

from cacm.config import settings
from cacm.utils.logging import get_logger

log = get_logger(__name__)

MODEL_DIR = settings.data_root / "models"
DETECTOR_COLS = [
    "score_D1_CUSUM",
    "score_D2_EWMA",
    "score_D3_3SIGMA_HORA",
    "score_D4_BENFORD",
    "score_D5_FRACIONAMENTO",
    "score_M1_ISOLATION_FOREST",
    "score_M2_AUTOENCODER",
]


def train_logistic_calibrator(df_alertas_rotulados: pd.DataFrame) -> Path:
    """Treina logística L2 sobre alertas com `status` ∈ {CONFIRMADO, FALSO_POSITIVO}."""
    df = df_alertas_rotulados[df_alertas_rotulados["status"].isin(["CONFIRMADO", "FALSO_POSITIVO"])]
    if len(df) < 50:
        raise ValueError("Histórico insuficiente para treinar calibrador (mín. 50 alertas rotulados).")
    y = (df["status"] == "CONFIRMADO").astype(int).to_numpy()
    X = df[DETECTOR_COLS].fillna(0.0).to_numpy()

    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)
    X_train, X_val, y_train, y_val = train_test_split(Xs, y, test_size=0.25, random_state=42, stratify=y)

    mlflow.set_experiment("cacm/logistic-calibrator")
    with mlflow.start_run() as run:
        model = LogisticRegression(penalty="l2", C=1.0, max_iter=1000)
        model.fit(X_train, y_train)
        auc = roc_auc_score(y_val, model.predict_proba(X_val)[:, 1])
        mlflow.log_metric("val_auc", auc)
        mlflow.log_params({"C": 1.0, "n_train": len(X_train)})

        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        artifact = MODEL_DIR / "logistic_calibrator.joblib"
        joblib.dump(
            {"model": model, "scaler": scaler, "cols": DETECTOR_COLS},
            artifact,
        )
        mlflow.log_artifact(str(artifact))
        log.info("calibrator.trained", run_id=run.info.run_id, val_auc=auc)
    return artifact


def apply_calibrator(df_scores: pd.DataFrame) -> np.ndarray:
    """Devolve probabilidade calibrada de confirmação para cada linha."""
    artifact = MODEL_DIR / "logistic_calibrator.joblib"
    bundle = joblib.load(artifact)
    X = bundle["scaler"].transform(df_scores[bundle["cols"]].fillna(0.0).to_numpy())
    return bundle["model"].predict_proba(X)[:, 1]
