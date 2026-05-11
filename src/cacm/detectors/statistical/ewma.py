"""D2 — Carta EWMA para ticket médio de PIX por associado.

EWMA(t) = lambda * x_t + (1 - lambda) * EWMA(t-1)
Limite de controle: mu0 ± L * sigma * sqrt(lambda / (2 - lambda) * (1 - (1-lambda)^(2t)))
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from cacm.config import settings


@dataclass
class EwmaResult:
    ewma: np.ndarray
    upper: np.ndarray
    lower: np.ndarray
    alerta: np.ndarray


def ewma(values: np.ndarray, lam: float | None = None, L: float | None = None) -> EwmaResult:
    lam_use = settings.ewma_lambda if lam is None else lam
    L_use = settings.ewma_L if L is None else L
    mu0 = float(np.mean(values))
    sigma = float(np.std(values, ddof=1)) if len(values) > 1 else 0.0
    sigma = sigma if sigma > 0 else 1.0
    n = len(values)
    z = np.zeros(n)
    upper = np.zeros(n)
    lower = np.zeros(n)
    for i in range(n):
        z[i] = lam_use * values[i] + (1 - lam_use) * (z[i - 1] if i > 0 else mu0)
        var_t = (lam_use / (2 - lam_use)) * (1 - (1 - lam_use) ** (2 * (i + 1)))
        upper[i] = mu0 + L_use * sigma * np.sqrt(var_t)
        lower[i] = mu0 - L_use * sigma * np.sqrt(var_t)
    alerta = (z > upper) | (z < lower)
    return EwmaResult(z, upper, lower, alerta)


def ewma_por_associado_pix(df_silver_pix: pd.DataFrame) -> pd.DataFrame:
    """Aplica EWMA ao ticket diário de PIX por associado.

    `df_silver_pix` deve conter: associado_id, dt, valor (já filtrado a tipos PIX).
    """
    diario = (
        df_silver_pix.groupby(["associado_id", "dt"])["valor"]
        .mean()
        .reset_index()
        .rename(columns={"valor": "ticket_medio"})
    )
    out = []
    for assoc, g in diario.sort_values("dt").groupby("associado_id"):
        if len(g) < 5:
            continue
        res = ewma(g["ticket_medio"].to_numpy())
        # score proporcional à distância normalizada ao limite
        denom = np.maximum(res.upper - res.lower, 1e-9)
        score = np.clip(np.abs(res.ewma - (res.upper + res.lower) / 2) / (denom / 2), 0.0, 1.0)
        out.append(
            pd.DataFrame(
                {
                    "associado_id": assoc,
                    "dt": g["dt"].to_numpy(),
                    "regra_id": "D2_EWMA",
                    "familia": "FRAUDE",
                    "alerta": res.alerta,
                    "score": score,
                    "motivo": np.where(res.alerta, "EWMA do ticket médio fora de controle", None),
                }
            )
        )
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()
