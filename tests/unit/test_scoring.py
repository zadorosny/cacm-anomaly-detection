"""Testes do scoring combinado."""

from __future__ import annotations

import pandas as pd

from cacm.alerts.scoring import combinar_scores, severidade_por_score


def test_severidade_bins():
    assert severidade_por_score(0.3) is None
    assert severidade_por_score(0.6) == "BAIXA"
    assert severidade_por_score(0.75) == "MEDIA"
    assert severidade_por_score(0.9) == "ALTA"
    assert severidade_por_score(0.97) == "CRITICA"


def test_combinar_scores_pondera_corretamente():
    df1 = pd.DataFrame(
        {
            "transacao_id": ["t1", "t2"],
            "regra_id": ["M1_ISOLATION_FOREST"] * 2,
            "familia": ["FRAUDE"] * 2,
            "alerta": [True, True],
            "score": [1.0, 0.8],
            "motivo": ["if", "if"],
        }
    )
    df2 = pd.DataFrame(
        {
            "transacao_id": ["t1"],
            "regra_id": ["D5_FRACIONAMENTO"],
            "familia": ["PLD"],
            "alerta": [True],
            "score": [0.9],
            "motivo": ["pld"],
        }
    )
    agg = combinar_scores(df1, df2)
    # t1 deve ter score combinado >= severidade BAIXA
    t1 = agg[agg["entidade"] == "t1"].iloc[0]
    assert t1["severidade"] in {"BAIXA", "MEDIA", "ALTA", "CRITICA"}
    assert "M1_ISOLATION_FOREST" in t1["regras"]
    assert "D5_FRACIONAMENTO" in t1["regras"]
