"""D1 — Carta CUSUM (Page, 1954) para volume diário por agência.

Detecta deslocamentos persistentes da média histórica. Para portfólio usamos a
versão tabular CUSUM dois-lados:
    S_h(t) = max(0, S_h(t-1) + (x_t - mu0) - k*sigma)
    S_l(t) = min(0, S_l(t-1) + (x_t - mu0) + k*sigma)
Sinal de alerta quando |S| > h * sigma.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from cacm.config import settings


@dataclass
class CusumResult:
    s_high: np.ndarray
    s_low: np.ndarray
    alerta: np.ndarray
    mu0: float
    sigma: float


def cusum(values: np.ndarray, k: float | None = None, h: float | None = None) -> CusumResult:
    """Executa CUSUM dois-lados em uma série temporal."""
    k_use = settings.cusum_k if k is None else k
    h_use = settings.cusum_h if h is None else h
    mu0 = float(np.mean(values))
    sigma = float(np.std(values, ddof=1)) if len(values) > 1 else 0.0
    sigma = sigma if sigma > 0 else 1.0
    n = len(values)
    sh = np.zeros(n)
    sl = np.zeros(n)
    for i in range(n):
        prev_h = sh[i - 1] if i > 0 else 0.0
        prev_l = sl[i - 1] if i > 0 else 0.0
        sh[i] = max(0.0, prev_h + (values[i] - mu0) - k_use * sigma)
        sl[i] = min(0.0, prev_l + (values[i] - mu0) + k_use * sigma)
    alerta = (sh > h_use * sigma) | (sl < -h_use * sigma)
    return CusumResult(sh, sl, alerta, mu0, sigma)


def cusum_por_agencia(df_ag_dia: pd.DataFrame) -> pd.DataFrame:
    """Aplica CUSUM em `qtd_tx` para cada agência. Retorna alertas (1 linha/dia/agência)."""
    out = []
    for ag, g in df_ag_dia.sort_values("dt").groupby("agencia_id"):
        res = cusum(g["qtd_tx"].to_numpy())
        score = np.clip(np.abs(res.s_high - res.s_low) / max(res.sigma * settings.cusum_h, 1.0), 0.0, 1.0)
        out.append(
            pd.DataFrame(
                {
                    "agencia_id": ag,
                    "dt": g["dt"].to_numpy(),
                    "regra_id": "D1_CUSUM",
                    "familia": "FRAUDE",
                    "alerta": res.alerta,
                    "score": score,
                    "motivo": np.where(
                        res.alerta,
                        f"CUSUM fora dos limites em {ag}",
                        None,
                    ),
                }
            )
        )
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()
