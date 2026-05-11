"""M2 — Autoencoder denso para reconstrução de perfil transacional.

Embedding mensal por associado (vetor agregado). Reconstrução com MSE; limiar
de alerta = percentil 99 do erro no conjunto de validação.
Arquitetura: 32 → 16 → 8 → 16 → 32, ReLU, MSE.
"""

from __future__ import annotations

from pathlib import Path

import mlflow
import numpy as np
import pandas as pd
import torch
from sklearn.preprocessing import StandardScaler
from torch import nn
from torch.utils.data import DataLoader, TensorDataset

from cacm.config import settings
from cacm.utils.logging import get_logger

log = get_logger(__name__)

MODEL_DIR = settings.data_root / "models"


class DenseAutoencoder(nn.Module):
    def __init__(self, in_dim: int) -> None:
        super().__init__()
        self.encoder = nn.Sequential(
            nn.Linear(in_dim, 16),
            nn.ReLU(),
            nn.Linear(16, 8),
            nn.ReLU(),
        )
        self.decoder = nn.Sequential(
            nn.Linear(8, 16),
            nn.ReLU(),
            nn.Linear(16, in_dim),
        )

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.decoder(self.encoder(x))


def _embedding_mensal(df_silver: pd.DataFrame) -> pd.DataFrame:
    df = df_silver.copy()
    df["dt_transacao"] = pd.to_datetime(df["dt_transacao"])
    df["ano_mes"] = df["dt_transacao"].dt.strftime("%Y-%m")
    agg = (
        df.groupby(["associado_id", "ano_mes"])
        .agg(
            qtd_tx=("valor", "size"),
            valor_total=("valor", "sum"),
            valor_medio=("valor", "mean"),
            valor_max=("valor", "max"),
            valor_std=("valor", "std"),
            hora_media=("dt_transacao", lambda s: s.dt.hour.mean()),
            destinos_unicos=("conta_destino", lambda s: s.nunique()),
            canais=("canal", lambda s: s.nunique()),
            pct_madrugada=("dt_transacao", lambda s: ((s.dt.hour < 6) | (s.dt.hour >= 22)).mean()),
        )
        .reset_index()
        .fillna(0.0)
    )
    return agg


FEATURES = [
    "qtd_tx",
    "valor_total",
    "valor_medio",
    "valor_max",
    "valor_std",
    "hora_media",
    "destinos_unicos",
    "canais",
    "pct_madrugada",
]


def train_autoencoder(
    df_silver: pd.DataFrame | None = None,
    epochs: int = 40,
    batch_size: int = 256,
    lr: float = 1e-3,
    seed: int = 42,
) -> Path:
    """Treina autoencoder e persiste pesos + scaler."""
    if df_silver is None:
        from cacm.detectors.ml.isolation_forest import _read_silver_pandas

        df_silver = _read_silver_pandas()

    torch.manual_seed(seed)
    emb = _embedding_mensal(df_silver)
    X = emb[FEATURES].to_numpy(dtype=np.float32)
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X).astype(np.float32)

    # 80/20 train/val
    n = len(X_scaled)
    perm = np.random.default_rng(seed).permutation(n)
    cut = int(0.8 * n)
    X_train = X_scaled[perm[:cut]]
    X_val = X_scaled[perm[cut:]]

    model = DenseAutoencoder(in_dim=X.shape[1])
    optim = torch.optim.Adam(model.parameters(), lr=lr)
    loss_fn = nn.MSELoss()
    loader = DataLoader(
        TensorDataset(torch.from_numpy(X_train)), batch_size=batch_size, shuffle=True
    )

    mlflow.set_experiment("cacm/autoencoder")
    with mlflow.start_run() as run:
        mlflow.log_params(
            {"epochs": epochs, "lr": lr, "batch_size": batch_size, "n_features": X.shape[1]}
        )
        for epoch in range(epochs):
            model.train()
            running = 0.0
            for (batch,) in loader:
                optim.zero_grad()
                rec = model(batch)
                loss = loss_fn(rec, batch)
                loss.backward()
                optim.step()
                running += loss.item() * batch.size(0)
            train_loss = running / len(X_train)
            model.eval()
            with torch.no_grad():
                rec_val = model(torch.from_numpy(X_val))
                val_loss = loss_fn(rec_val, torch.from_numpy(X_val)).item()
            mlflow.log_metrics({"train_loss": train_loss, "val_loss": val_loss}, step=epoch)

        # limiar = p99 do erro no validation set
        with torch.no_grad():
            rec_val = model(torch.from_numpy(X_val)).numpy()
            err_val = np.mean((rec_val - X_val) ** 2, axis=1)
        threshold = float(np.percentile(err_val, 99))
        mlflow.log_metric("threshold_p99", threshold)

        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        artifact = MODEL_DIR / "autoencoder.pt"
        torch.save(
            {
                "state_dict": model.state_dict(),
                "in_dim": X.shape[1],
                "threshold": threshold,
                "features": FEATURES,
            },
            artifact,
        )
        # scaler separado (joblib)
        import joblib

        joblib.dump(scaler, MODEL_DIR / "autoencoder_scaler.joblib")
        mlflow.log_artifact(str(artifact))
        log.info("ae.trained", run_id=run.info.run_id, threshold=threshold)
    return artifact


def score_autoencoder(df_silver: pd.DataFrame) -> pd.DataFrame:
    """Score por (associado, mês) — útil para detectar 'escalada' (F5)."""
    import joblib

    bundle = torch.load(MODEL_DIR / "autoencoder.pt", weights_only=False)
    scaler = joblib.load(MODEL_DIR / "autoencoder_scaler.joblib")
    emb = _embedding_mensal(df_silver)
    X = scaler.transform(emb[bundle["features"]].to_numpy(dtype=np.float32)).astype(np.float32)
    model = DenseAutoencoder(in_dim=bundle["in_dim"])
    model.load_state_dict(bundle["state_dict"])
    model.eval()
    with torch.no_grad():
        rec = model(torch.from_numpy(X)).numpy()
    err = np.mean((rec - X) ** 2, axis=1)
    score = np.clip(err / max(bundle["threshold"] * 2, 1e-9), 0.0, 1.0)
    alerta = err > bundle["threshold"]
    return pd.DataFrame(
        {
            "associado_id": emb["associado_id"].to_numpy(),
            "ano_mes": emb["ano_mes"].to_numpy(),
            "regra_id": "M2_AUTOENCODER",
            "familia": "FRAUDE",
            "alerta": alerta,
            "score": score,
            "motivo": np.where(alerta, "Autoencoder: erro de reconstrução acima do p99", None),
        }
    )
