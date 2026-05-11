"""Injeta padrões fraudulentos rotulados (ground truth) no dataset bronze.

Padrões cobertos (alinhados aos detectores em `detectors/`):
- F1 SMURFING: múltiplas transações abaixo de R$10k em 24h pelo mesmo associado
- F2 BURST_NOTURNO: rajada de transações em horário não comercial por um operador
- F3 BENFORD_VIOLATION: valores manipulados (concentração de primeiro dígito 5+)
- F4 GEO_OUTLIER: transação digital com geolocalização muito distante do padrão
- F5 ESCALADA: associado cujo ticket médio sobe gradualmente (M2 autoencoder)

Cada fraude marca `fraude_label=True` e `fraude_tipo=<código>`.
"""

from __future__ import annotations

import uuid
from datetime import timedelta

import numpy as np
import pandas as pd

from cacm.config import settings


def injetar_fraudes(
    df: pd.DataFrame,
    dims,
    rng: np.random.Generator,
    fracao: float = 0.005,
) -> pd.DataFrame:
    """Injeta ~`fracao` * len(df) transações fraudulentas adicionais."""
    n_target = int(len(df) * fracao)
    novas: list[pd.DataFrame] = []

    novas.append(_smurfing(df, dims, rng, n_assoc=max(1, n_target // 30)))
    novas.append(_burst_noturno(df, dims, rng, n_ops=max(1, n_target // 100)))
    novas.append(_benford_violation(df, dims, rng, n=max(50, n_target // 4)))
    df_geo = _geo_outlier(df, rng, n=max(20, n_target // 10))

    extras = pd.concat([x for x in novas if not x.empty], ignore_index=True)
    df_out = pd.concat([df, extras, df_geo], ignore_index=True)

    # Marca escalada (não cria linhas — apenas altera trajetória de alguns associados existentes)
    df_out = _escalada(df_out, rng, n_assoc=max(5, n_target // 50))

    return df_out


def _empty_like(df: pd.DataFrame) -> pd.DataFrame:
    return df.iloc[0:0].copy()


def _smurfing(df: pd.DataFrame, dims, rng: np.random.Generator, n_assoc: int) -> pd.DataFrame:
    """Cria fracionamento PIX: N transações <R$10k em 24h somando >R$10k."""
    limite = settings.structuring_limit_brl
    assoc_pool = (
        dims.associados["associado_id"]
        .sample(n=n_assoc, random_state=int(rng.integers(0, 10**6)))
        .to_numpy()
    )
    rows = []
    template = df.iloc[0].to_dict()
    for assoc in assoc_pool:
        n_split = int(rng.integers(3, 8))
        base_ts = pd.Timestamp(df["dt_transacao"].sample(1).iloc[0]).normalize() + pd.Timedelta(
            hours=int(rng.integers(8, 20))
        )
        for _ in range(n_split):
            valor = float(rng.uniform(limite * 0.55, limite * 0.95))
            row = template.copy()
            row.update(
                transacao_id=str(uuid.uuid4()),
                tipo="PIX_OUT",
                valor=round(valor, 2),
                dt_transacao=base_ts + timedelta(minutes=int(rng.integers(5, 600))),
                conta_origem=f"CT{abs(hash(assoc)) % 10**10:010d}",
                conta_destino=f"CT{int(rng.integers(0, 10**10)):010d}",
                associado_id=assoc,
                agencia_id=rng.choice(dims.agencias["agencia_id"]),
                operador_id=None,
                canal="APP",
                ip_origem=".".join(str(int(rng.integers(1, 254))) for _ in range(4)),
                geo_lat=float(rng.normal(-16.6, 0.5)),
                geo_lon=float(rng.normal(-49.3, 0.5)),
                dt_ingestao=pd.Timestamp.now(),
                fraude_label=True,
                fraude_tipo="F1_SMURFING",
            )
            rows.append(row)
    return pd.DataFrame(rows) if rows else _empty_like(df)


def _burst_noturno(df: pd.DataFrame, dims, rng: np.random.Generator, n_ops: int) -> pd.DataFrame:
    """Operador com rajada de transações em madrugada."""
    operadores = (
        dims.operadores["operador_id"]
        .sample(n=n_ops, random_state=int(rng.integers(0, 10**6)))
        .to_numpy()
    )
    rows = []
    template = df.iloc[0].to_dict()
    for op in operadores:
        ag_lotacao = dims.operadores.loc[
            dims.operadores["operador_id"] == op, "agencia_lotacao"
        ].iloc[0]
        base_ts = pd.Timestamp(df["dt_transacao"].sample(1).iloc[0]).normalize() + pd.Timedelta(
            hours=int(rng.integers(2, 5))
        )
        n_burst = int(rng.integers(8, 20))
        for _ in range(n_burst):
            valor = float(rng.lognormal(7.0, 1.0))
            assoc = rng.choice(dims.associados["associado_id"])
            row = template.copy()
            row.update(
                transacao_id=str(uuid.uuid4()),
                tipo=rng.choice(["TED", "DOC", "PIX_OUT"]),
                valor=round(valor, 2),
                dt_transacao=base_ts + timedelta(minutes=int(rng.integers(1, 60))),
                conta_origem=f"CT{abs(hash(assoc)) % 10**10:010d}",
                conta_destino=f"CT{int(rng.integers(0, 10**10)):010d}",
                associado_id=assoc,
                agencia_id=ag_lotacao,
                operador_id=op,
                canal="AGENCIA",
                ip_origem=None,
                geo_lat=np.nan,
                geo_lon=np.nan,
                dt_ingestao=pd.Timestamp.now(),
                fraude_label=True,
                fraude_tipo="F2_BURST_NOTURNO",
            )
            rows.append(row)
    return pd.DataFrame(rows) if rows else _empty_like(df)


def _benford_violation(df: pd.DataFrame, dims, rng: np.random.Generator, n: int) -> pd.DataFrame:
    """Valores manipulados com primeiro dígito enviesado para 5–9."""
    rows = []
    template = df.iloc[0].to_dict()
    for _ in range(n):
        primeiro = int(rng.choice([5, 6, 7, 8, 9], p=[0.30, 0.20, 0.20, 0.15, 0.15]))
        magnitude = int(rng.integers(2, 5))
        resto = rng.integers(0, 10**magnitude)
        valor = float(f"{primeiro}{resto:0{magnitude}d}.{int(rng.integers(0,100)):02d}")
        assoc = rng.choice(dims.associados["associado_id"])
        row = template.copy()
        row.update(
            transacao_id=str(uuid.uuid4()),
            tipo="TED",
            valor=valor,
            dt_transacao=pd.Timestamp(df["dt_transacao"].sample(1).iloc[0]),
            conta_origem=f"CT{abs(hash(assoc)) % 10**10:010d}",
            conta_destino=f"CT{int(rng.integers(0, 10**10)):010d}",
            associado_id=assoc,
            agencia_id=rng.choice(dims.agencias["agencia_id"]),
            operador_id=None,
            canal="INTERNET_BANKING",
            ip_origem=".".join(str(int(rng.integers(1, 254))) for _ in range(4)),
            geo_lat=float(rng.normal(-16.6, 1.0)),
            geo_lon=float(rng.normal(-49.3, 1.0)),
            dt_ingestao=pd.Timestamp.now(),
            fraude_label=True,
            fraude_tipo="F3_BENFORD_VIOLATION",
        )
        rows.append(row)
    return pd.DataFrame(rows) if rows else _empty_like(df)


def _geo_outlier(df: pd.DataFrame, rng: np.random.Generator, n: int) -> pd.DataFrame:
    """Marca transações digitais existentes como geo_outlier (lat/lon longe)."""
    digital_idx = df.index[df["canal"].isin(["APP", "INTERNET_BANKING", "API"])]
    if len(digital_idx) == 0:
        return _empty_like(df)
    sel = rng.choice(digital_idx, size=min(n, len(digital_idx)), replace=False)
    df_out = df.loc[sel].copy()
    # Coloca em outro país (Tóquio)
    df_out["geo_lat"] = rng.normal(35.6, 0.3, size=len(df_out))
    df_out["geo_lon"] = rng.normal(139.6, 0.3, size=len(df_out))
    df_out["fraude_label"] = True
    df_out["fraude_tipo"] = "F4_GEO_OUTLIER"
    df_out["transacao_id"] = [str(uuid.uuid4()) for _ in range(len(df_out))]
    return df_out


def _escalada(df: pd.DataFrame, rng: np.random.Generator, n_assoc: int) -> pd.DataFrame:
    """Aumenta gradualmente o ticket de alguns associados ao longo do tempo (rotula EWMA)."""
    assocs = rng.choice(df["associado_id"].unique(), size=n_assoc, replace=False)
    df = df.sort_values("dt_transacao").reset_index(drop=True)
    for a in assocs:
        mask = df["associado_id"].eq(a) & df["tipo"].str.startswith("PIX")
        idx = df.index[mask]
        if len(idx) < 5:
            continue
        ramp = np.linspace(1.0, 6.0, num=len(idx))  # fator multiplicativo crescente
        df.loc[idx, "valor"] = (df.loc[idx, "valor"].to_numpy() * ramp).round(2)
        df.loc[idx, "fraude_label"] = True
        df.loc[idx, "fraude_tipo"] = df.loc[idx, "fraude_tipo"].fillna("F5_ESCALADA")
    return df
