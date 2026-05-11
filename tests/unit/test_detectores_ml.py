"""Testes leves dos detectores ML — usam dados sintéticos pequenos."""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def silver_fake():
    rng = np.random.default_rng(0)
    n = 500
    base = datetime(2026, 4, 1)
    return pd.DataFrame(
        {
            "transacao_id": [f"t{i}" for i in range(n)],
            "associado_id": rng.choice([f"A{i}" for i in range(30)], size=n),
            "agencia_id": rng.choice([f"AG{i:02d}" for i in range(5)], size=n),
            "valor": rng.lognormal(5.0, 1.0, size=n).round(2),
            "dt_transacao": [base + timedelta(minutes=int(x)) for x in rng.integers(0, 60 * 24 * 30, size=n)],
            "conta_destino": rng.choice([f"C{i}" for i in range(50)], size=n),
            "canal": rng.choice(["APP", "AGENCIA", "INTERNET_BANKING"], size=n),
            "geo_lat": rng.normal(-16.6, 0.5, size=n),
            "geo_lon": rng.normal(-49.3, 0.5, size=n),
            "score_risco": rng.uniform(0, 1, size=n),
            "dt_adesao": [datetime(2020, 1, 1)] * n,
        }
    )


def test_build_features_produces_all_columns(silver_fake):
    from cacm.detectors.ml.features import FEATURE_COLS, build_features
    feats = build_features(silver_fake)
    assert set(FEATURE_COLS).issubset(feats.columns)
    assert len(feats) == len(silver_fake)
    assert feats[FEATURE_COLS].isna().sum().sum() == 0


@pytest.mark.slow
def test_autoencoder_train_smoke(silver_fake, tmp_path, monkeypatch):
    from cacm.detectors.ml import autoencoder
    monkeypatch.setattr(autoencoder, "MODEL_DIR", tmp_path)
    artifact = autoencoder.train_autoencoder(silver_fake, epochs=2, batch_size=16)
    assert artifact.exists()
