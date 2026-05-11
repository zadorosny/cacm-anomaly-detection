"""Gera tabelas de dimensão (agências, associados, operadores, calendário).

Cenário fictício baseado em Goiás (UFs/municípios reais, dados simulados).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
import pandas as pd
from faker import Faker

# Municípios fictícios de Goiás (subset realista)
GO_MUNICIPIOS = [
    "Goiânia",
    "Aparecida de Goiânia",
    "Anápolis",
    "Rio Verde",
    "Luziânia",
    "Águas Lindas de Goiás",
    "Valparaíso de Goiás",
    "Trindade",
    "Formosa",
    "Novo Gama",
    "Senador Canedo",
    "Itumbiara",
    "Catalão",
    "Jataí",
    "Planaltina",
    "Caldas Novas",
    "Cidade Ocidental",
    "Goianésia",
    "Inhumas",
    "Mineiros",
]

SEGMENTOS_ASSOCIADO = ["PF_VAREJO", "PF_PREMIUM", "PJ_MICRO", "PJ_PEQUENA", "PJ_MEDIA"]
PORTES_AGENCIA = ["PEQUENA", "MEDIA", "GRANDE"]
FUNCOES_OPERADOR = ["CAIXA", "GERENTE_RELACIONAMENTO", "GERENTE_GERAL", "BACK_OFFICE"]


@dataclass
class Dimensoes:
    agencias: pd.DataFrame
    associados: pd.DataFrame
    operadores: pd.DataFrame
    calendario: pd.DataFrame


def gerar_dim_agencias(n: int, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    fake = Faker("pt_BR")
    Faker.seed(seed)
    rows = []
    for i in range(n):
        rows.append(
            {
                "agencia_id": f"AG{i:04d}",
                "municipio": rng.choice(GO_MUNICIPIOS),
                "uf": "GO",
                "central": "CENTRAL_BRASIL_CENTRAL",
                "porte": rng.choice(PORTES_AGENCIA, p=[0.6, 0.3, 0.1]),
                "endereco": fake.street_address(),
            }
        )
    return pd.DataFrame(rows)


def gerar_dim_associados(n: int, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    hoje = date.today()
    rows = []
    for i in range(n):
        anos_adesao = rng.integers(0, 20)
        rows.append(
            {
                "associado_id": f"AS{i:07d}",
                "segmento": rng.choice(SEGMENTOS_ASSOCIADO, p=[0.55, 0.15, 0.15, 0.10, 0.05]),
                "dt_adesao": hoje - timedelta(days=int(anos_adesao * 365)),
                "score_risco": float(np.clip(rng.normal(0.3, 0.15), 0.0, 1.0)),
                "uf_residencia": "GO",
            }
        )
    return pd.DataFrame(rows)


def gerar_dim_operadores(n: int, agencias: pd.DataFrame, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    hoje = date.today()
    rows = []
    agencia_ids = agencias["agencia_id"].to_numpy()
    for i in range(n):
        anos_admissao = rng.integers(0, 15)
        rows.append(
            {
                "operador_id": f"OP{i:05d}",
                "funcao": rng.choice(FUNCOES_OPERADOR, p=[0.5, 0.25, 0.05, 0.2]),
                "dt_admissao": hoje - timedelta(days=int(anos_admissao * 365)),
                "agencia_lotacao": str(rng.choice(agencia_ids)),
            }
        )
    return pd.DataFrame(rows)


def gerar_dim_calendario(dt_inicio: date, dt_fim: date) -> pd.DataFrame:
    dias = pd.date_range(dt_inicio, dt_fim, freq="D")
    df = pd.DataFrame({"dt": dias.date})
    df["ano"] = pd.to_datetime(df["dt"]).dt.year
    df["mes"] = pd.to_datetime(df["dt"]).dt.month
    df["dia_semana"] = pd.to_datetime(df["dt"]).dt.dayofweek  # 0=segunda
    df["dia_util"] = df["dia_semana"] < 5
    # Feriados nacionais aproximados (lista mínima — projeto de portfólio)
    feriados_fixos = {(1, 1), (4, 21), (5, 1), (9, 7), (10, 12), (11, 2), (11, 15), (12, 25)}
    df["feriado"] = df.apply(
        lambda r: (r["mes"], pd.to_datetime(r["dt"]).day) in feriados_fixos, axis=1
    )
    df["dia_util"] = df["dia_util"] & ~df["feriado"]
    return df


def gerar_dimensoes(
    n_agencias: int,
    n_associados: int,
    n_operadores: int,
    dt_inicio: date,
    dt_fim: date,
    seed: int = 42,
) -> Dimensoes:
    agencias = gerar_dim_agencias(n_agencias, seed)
    associados = gerar_dim_associados(n_associados, seed + 1)
    operadores = gerar_dim_operadores(n_operadores, agencias, seed + 2)
    calendario = gerar_dim_calendario(dt_inicio, dt_fim)
    return Dimensoes(
        agencias=agencias, associados=associados, operadores=operadores, calendario=calendario
    )
