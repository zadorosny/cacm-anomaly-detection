"""D4 — Lei de Benford no primeiro dígito de valores.

Frequência esperada de primeiro dígito d: log10(1 + 1/d), d ∈ {1..9}.
Comparação por:
- Qui-quadrado (Pearson)
- MAD (Mean Absolute Deviation) — Nigrini (2012)

Aplicado a janelas (segmento × mês) para evitar diluir sinais.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from scipy import stats

BENFORD_PROBS = np.array([np.log10(1 + 1 / d) for d in range(1, 10)])


@dataclass
class BenfordResult:
    contagem_obs: np.ndarray
    freq_obs: np.ndarray
    chi2: float
    p_value: float
    mad: float


def primeiro_digito(valores: np.ndarray) -> np.ndarray:
    """Extrai o primeiro dígito significativo (1-9)."""
    v = np.abs(np.asarray(valores, dtype=float))
    v = v[v > 0]
    expoente = np.floor(np.log10(v)).astype(int)
    return (v / (10.0**expoente)).astype(int)


def benford_test(valores: np.ndarray) -> BenfordResult:
    digs = primeiro_digito(valores)
    if len(digs) < 30:
        return BenfordResult(
            np.zeros(9, dtype=int), np.zeros(9), float("nan"), float("nan"), float("nan")
        )
    contagem = np.array([(digs == d).sum() for d in range(1, 10)], dtype=int)
    freq = contagem / contagem.sum()
    esperado = BENFORD_PROBS * contagem.sum()
    chi2, p = stats.chisquare(contagem, esperado)
    mad = float(np.mean(np.abs(freq - BENFORD_PROBS)))
    return BenfordResult(contagem, freq, float(chi2), float(p), mad)


def benford_por_segmento(df: pd.DataFrame, key: str = "tipo", min_n: int = 100) -> pd.DataFrame:
    """Aplica teste de Benford agrupado por `key` (ex: tipo, segmento, agência)."""
    rows = []
    for k, g in df.groupby(key):
        if len(g) < min_n:
            continue
        res = benford_test(g["valor"].to_numpy())
        alerta = (res.mad > 0.015) or (res.p_value < 0.01)
        # MAD bins de Nigrini: <0.006 conformidade alta; 0.006-0.012 aceitável; >0.015 não conforme
        score = (
            float(np.clip((res.mad - 0.006) / 0.020, 0.0, 1.0)) if not np.isnan(res.mad) else 0.0
        )
        rows.append(
            {
                "chave": k,
                "regra_id": "D4_BENFORD",
                "familia": "FRAUDE",
                "alerta": bool(alerta),
                "chi2": res.chi2,
                "p_value": res.p_value,
                "mad": res.mad,
                "score": score,
                "motivo": (
                    f"Benford: MAD={res.mad:.4f}, p={res.p_value:.4f} ({key}={k})"
                    if alerta
                    else None
                ),
            }
        )
    return pd.DataFrame(rows)
