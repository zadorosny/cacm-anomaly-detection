"""M1 — Isolation Forest multivariado por canal.

Treinamento não-supervisionado sobre 90 dias históricos. Score normalizado 0–1
(0 = normal, 1 = altamente anômalo).
Liu, Ting, Zhou (2008).
"""

from __future__ import annotations

from pathlib import Path

import joblib
import mlflow
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

from cacm.config import settings
from cacm.detectors.ml.features import FEATURE_COLS, build_features
from cacm.utils.logging import get_logger

log = get_logger(__name__)

MODEL_DIR = settings.data_root / "models"


def _read_silver_pandas() -> pd.DataFrame:
    """Lê silver e converte a pandas (assumindo dataset cabendo em memória).

    Para volumes maiores, troca-se por amostragem ou MLlib.
    """
    from cacm.utils.spark_session import get_spark

    spark = get_spark("if-train")
    sdf = spark.read.parquet(str(settings.silver_path / "transacoes_silver"))
    return sdf.toPandas()


def train_isolation_forest(
    df_silver: pd.DataFrame | None = None,
    contamination: float = 0.01,
    n_estimators: int = 200,
    random_state: int = 42,
) -> Path:
    """Treina Isolation Forest, registra no MLflow e persiste em `data/models/`."""
    if df_silver is None:
        df_silver = _read_silver_pandas()

    feats = build_features(df_silver)
    X = feats[FEATURE_COLS].to_numpy()
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    mlflow.set_experiment("cacm/isolation-forest")
    with mlflow.start_run() as run:
        mlflow.log_params(
            {"contamination": contamination, "n_estimators": n_estimators, "random_state": random_state}
        )
        model = IsolationForest(
            n_estimators=n_estimators,
            contamination=contamination,
            random_state=random_state,
            n_jobs=-1,
        )
        model.fit(X_scaled)

        scores = -model.score_samples(X_scaled)
        score_norm = (scores - scores.min()) / max(scores.ptp(), 1e-9)
        mlflow.log_metric("score_p99", float(np.percentile(score_norm, 99)))

        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        artifact = MODEL_DIR / "isolation_forest.joblib"
        joblib.dump({"model": model, "scaler": scaler, "feature_cols": FEATURE_COLS}, artifact)
        mlflow.log_artifact(str(artifact))
        log.info("if.trained", run_id=run.info.run_id, n=len(feats))
    return artifact


def score_isolation_forest(df_silver: pd.DataFrame, artifact: Path | None = None) -> pd.DataFrame:
    """Aplica modelo treinado e retorna scores por transação."""
    artifact = artifact or (MODEL_DIR / "isolation_forest.joblib")
    bundle = joblib.load(artifact)
    feats = build_features(df_silver)
    X = bundle["scaler"].transform(feats[bundle["feature_cols"]].to_numpy())
    raw = -bundle["model"].score_samples(X)
    score = (raw - raw.min()) / max(raw.ptp(), 1e-9)
    alerta = score >= np.percentile(score, 99)
    return pd.DataFrame(
        {
            "transacao_id": feats["transacao_id"].to_numpy(),
            "regra_id": "M1_ISOLATION_FOREST",
            "familia": "FRAUDE",
            "alerta": alerta,
            "score": score,
            "motivo": np.where(alerta, "Isolation Forest: padrão multivariado anômalo", None),
        }
    )
