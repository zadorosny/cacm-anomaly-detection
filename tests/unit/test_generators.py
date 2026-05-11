"""Testes do gerador de dados sintéticos e injetor de fraudes."""

from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pytest

from cacm.generators.dimensoes import gerar_dimensoes
from cacm.generators.fraudes_sinteticas import injetar_fraudes


@pytest.fixture
def dims_small():
    hoje = date.today()
    return gerar_dimensoes(
        n_agencias=5,
        n_associados=200,
        n_operadores=20,
        dt_inicio=hoje - timedelta(days=7),
        dt_fim=hoje,
        seed=7,
    )


def test_dimensoes_shapes(dims_small):
    assert len(dims_small.agencias) == 5
    assert len(dims_small.associados) == 200
    assert len(dims_small.operadores) == 20
    assert len(dims_small.calendario) == 8  # 7 dias + hoje
    assert set(dims_small.agencias.columns) >= {"agencia_id", "municipio", "uf", "porte"}


def test_calendario_marca_fins_de_semana(dims_small):
    cal = dims_small.calendario
    sabados = cal[cal["dia_semana"] == 5]
    assert not sabados["dia_util"].any()


def test_injetor_de_fraudes_marca_label():
    import pandas as pd

    rng = np.random.default_rng(0)
    hoje = date.today()
    dims = gerar_dimensoes(5, 100, 10, hoje - timedelta(days=3), hoje, seed=11)
    base = pd.DataFrame(
        {
            "transacao_id": ["t0"],
            "tipo": ["PIX_OUT"],
            "valor": [100.0],
            "dt_transacao": [pd.Timestamp("2026-05-01 12:00")],
            "conta_origem": ["CT0"],
            "conta_destino": ["CT1"],
            "associado_id": ["AS0000000"],
            "agencia_id": ["AG0000"],
            "operador_id": [None],
            "canal": ["APP"],
            "ip_origem": ["1.1.1.1"],
            "geo_lat": [-16.6],
            "geo_lon": [-49.3],
            "dt_ingestao": [pd.Timestamp.now()],
            "fraude_label": [False],
            "fraude_tipo": [None],
        }
    )
    # repete base p/ ter linhas suficientes para escalada/geo
    base = pd.concat([base.assign(transacao_id=f"t{i}") for i in range(500)], ignore_index=True)
    out = injetar_fraudes(base, dims, rng, fracao=0.05)
    assert out["fraude_label"].sum() > 0
    assert out.loc[out["fraude_label"], "fraude_tipo"].notna().all()
