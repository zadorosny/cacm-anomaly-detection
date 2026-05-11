"""D3 — Limites 3-sigma para concentração horária por agência."""

from __future__ import annotations

import numpy as np
import pandas as pd

from cacm.config import settings


def three_sigma_concentracao_horaria(df: pd.DataFrame) -> pd.DataFrame:
    """Detecta horas com volume além de mu ± k*sigma para cada agência.

    `df` deve conter: agencia_id, dt_transacao, transacao_id (ou linha=1 tx).
    """
    df = df.assign(
        hora=pd.to_datetime(df["dt_transacao"]).dt.hour,
        dt=pd.to_datetime(df["dt_transacao"]).dt.date,
    )
    pivot = df.groupby(["agencia_id", "dt", "hora"]).size().reset_index(name="qtd")

    k = settings.three_sigma_k
    out = []
    for ag, g in pivot.groupby("agencia_id"):
        mu = g["qtd"].mean()
        sd = g["qtd"].std(ddof=1) or 1.0
        upper = mu + k * sd
        lower = max(0.0, mu - k * sd)
        alerta = (g["qtd"] > upper) | (g["qtd"] < lower)
        score = np.clip(np.abs(g["qtd"] - mu) / (k * sd), 0.0, 1.0)
        out.append(
            pd.DataFrame(
                {
                    "agencia_id": ag,
                    "dt": g["dt"].to_numpy(),
                    "hora": g["hora"].to_numpy(),
                    "regra_id": "D3_3SIGMA_HORA",
                    "familia": "CONTROLE_INTERNO",
                    "alerta": alerta.to_numpy(),
                    "score": score.to_numpy(),
                    "motivo": np.where(
                        alerta,
                        "Volume horário fora de 3 desvios padrão",
                        None,
                    ),
                }
            )
        )
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()
