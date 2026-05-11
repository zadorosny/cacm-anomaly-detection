"""Feature engineering compartilhado pelos detectores ML."""

from __future__ import annotations

import numpy as np
import pandas as pd

FEATURE_COLS = [
    "valor",
    "log_valor",
    "hora",
    "dia_semana",
    "freq_7d",
    "freq_30d",
    "destinos_unicos_30d",
    "idade_conta_dias",
    "score_risco_assoc",
    "ratio_valor_ticket_medio",
    "distancia_geo_padrao",
]


def build_features(df_silver: pd.DataFrame) -> pd.DataFrame:
    """Constrói matriz de features a partir do silver enriquecido.

    Espera colunas: transacao_id, associado_id, valor, dt_transacao, geo_lat,
    geo_lon, score_risco, dt_adesao.
    """
    df = df_silver.copy()
    df["dt_transacao"] = pd.to_datetime(df["dt_transacao"])
    df["hora"] = df["dt_transacao"].dt.hour
    df["dia_semana"] = df["dt_transacao"].dt.dayofweek
    df["log_valor"] = np.log1p(df["valor"].clip(lower=0.01))

    # Janelas por associado
    df = df.sort_values(["associado_id", "dt_transacao"]).reset_index(drop=True)
    df["freq_7d"] = df.groupby("associado_id")["dt_transacao"].transform(
        lambda s: s.rolling("7D", on=s).count() if False else _rolling_count(s, "7D")
    )
    df["freq_30d"] = df.groupby("associado_id")["dt_transacao"].transform(
        lambda s: _rolling_count(s, "30D")
    )

    # Destinos únicos acumulados por associado (aproximação leve da janela 30d)
    df["destinos_unicos_30d"] = df.groupby("associado_id")["conta_destino"].transform(
        _running_unique_count
    )

    # Idade da conta
    df["dt_adesao"] = pd.to_datetime(df.get("dt_adesao"), errors="coerce")
    df["idade_conta_dias"] = (df["dt_transacao"] - df["dt_adesao"]).dt.days.fillna(0).clip(lower=0)

    df["score_risco_assoc"] = df.get("score_risco", 0.3).fillna(0.3)

    # Ratio do valor com o ticket médio do associado
    ticket_medio = df.groupby("associado_id")["valor"].transform("mean")
    df["ratio_valor_ticket_medio"] = (df["valor"] / ticket_medio.replace(0, np.nan)).fillna(1.0)

    # Distância geográfica do centróide do associado
    lat_med = df.groupby("associado_id")["geo_lat"].transform("median")
    lon_med = df.groupby("associado_id")["geo_lon"].transform("median")
    df["distancia_geo_padrao"] = np.sqrt(
        (df["geo_lat"].fillna(lat_med) - lat_med).pow(2)
        + (df["geo_lon"].fillna(lon_med) - lon_med).pow(2)
    ).fillna(0.0)

    return df[["transacao_id", *FEATURE_COLS]].fillna(0.0)


def _rolling_count(series: pd.Series, window: str) -> np.ndarray:
    """Conta eventos dentro de janela temporal."""
    s = pd.Series(1, index=pd.to_datetime(series.to_numpy()))
    return s.rolling(window).count().to_numpy()


def _running_unique_count(series: pd.Series) -> np.ndarray:
    """Cumulative count of distinct values up to each position."""
    seen: set = set()
    out = np.empty(len(series), dtype=np.int64)
    for i, v in enumerate(series.to_numpy()):
        if v is not None and not (isinstance(v, float) and np.isnan(v)):
            seen.add(v)
        out[i] = len(seen)
    return out
