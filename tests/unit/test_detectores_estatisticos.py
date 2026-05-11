"""Testes unitários dos detectores estatísticos."""

from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from cacm.detectors.statistical.benford import (
    BENFORD_PROBS,
    benford_test,
    primeiro_digito,
)
from cacm.detectors.statistical.cusum import cusum
from cacm.detectors.statistical.ewma import ewma
from cacm.detectors.statistical.structuring import detectar_fracionamento


def test_cusum_detecta_shift_persistente():
    rng = np.random.default_rng(0)
    estavel = rng.normal(100, 5, size=50)
    shift = rng.normal(140, 5, size=20)  # shift +8 sigma
    res = cusum(np.concatenate([estavel, shift]))
    assert res.alerta[-1]
    assert not res.alerta[10]


def test_ewma_aceita_serie_estavel():
    rng = np.random.default_rng(1)
    serie = rng.normal(50, 2, size=100)
    res = ewma(serie)
    # menos de 5% de falsos alertas numa série estável
    assert res.alerta.mean() < 0.05


def test_primeiro_digito():
    assert list(primeiro_digito(np.array([12.5, 7.3, 1500, 0.045]))) == [1, 7, 1, 4]


def test_benford_aceita_distribuicao_benford():
    rng = np.random.default_rng(42)
    # amostra com primeiro dígito Benford
    digs = rng.choice(np.arange(1, 10), size=2000, p=BENFORD_PROBS)
    valores = digs * 10.0 ** rng.integers(0, 4, size=2000) + rng.uniform(0, 1, size=2000)
    res = benford_test(valores)
    assert res.p_value > 0.01
    assert res.mad < 0.02


def test_benford_detecta_manipulacao():
    # forçar primeiro dígito alto (típico de fraude)
    rng = np.random.default_rng(0)
    digs = rng.choice([5, 6, 7, 8, 9], size=2000)
    valores = digs * 10.0 ** rng.integers(1, 4, size=2000)
    res = benford_test(valores)
    assert res.mad > 0.05


def test_fracionamento_detecta_smurfing():
    base = datetime(2026, 5, 1, 9, 0)
    rows = []
    for i in range(5):
        rows.append(
            {
                "transacao_id": f"t{i}",
                "associado_id": "A1",
                "dt_transacao": base + timedelta(hours=i * 2),
                "tipo": "PIX_OUT",
                "valor": 4_000.0,
            }
        )
    df = pd.DataFrame(rows)
    out = detectar_fracionamento(df, limite_brl=10_000, window_h=24)
    assert not out.empty
    assert out["alerta"].all()
