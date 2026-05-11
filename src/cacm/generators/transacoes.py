"""Gerador de transações sintéticas realistas.

Modelagem:
- Valor: lognormal por tipo de transação (PIX ~ menor, TED ~ maior)
- Hora do dia: bimodal (manhã 10-12h e tarde 14-17h), com baixa atividade noite/madrugada
- Dia da semana: dia útil > sábado > domingo
- Sazonalidade: dia 5 e 30 (folha/aluguel) com pico

Saída: tabela BRONZE em Parquet particionada por dt_ingestao/agencia_id.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, time, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from cacm.config import settings
from cacm.generators.dimensoes import gerar_dimensoes
from cacm.generators.fraudes_sinteticas import injetar_fraudes
from cacm.utils.logging import get_logger

log = get_logger(__name__)

TIPOS = ["PIX_OUT", "PIX_IN", "TED", "DOC", "SAQUE", "DEPOSITO"]
TIPO_PROBS = [0.40, 0.30, 0.10, 0.05, 0.10, 0.05]

CANAIS = ["APP", "INTERNET_BANKING", "ATM", "AGENCIA", "API"]
CANAL_PROBS = [0.55, 0.20, 0.10, 0.10, 0.05]

# Lognormal params (mu, sigma) por tipo — produz BRL realistas
VALOR_PARAMS = {
    "PIX_OUT": (5.0, 1.2),  # ~R$ 150 mediana
    "PIX_IN": (5.2, 1.2),
    "TED": (7.5, 1.3),  # ~R$ 1800 mediana
    "DOC": (7.0, 1.2),
    "SAQUE": (5.6, 0.9),  # ~R$ 270
    "DEPOSITO": (5.8, 1.0),
}


def _amostrar_hora(rng: np.random.Generator) -> time:
    # Mistura: 70% horário comercial, 25% extra-comercial diurno, 5% madrugada
    bucket = rng.choice([0, 1, 2], p=[0.70, 0.25, 0.05])
    if bucket == 0:
        h = int(
            rng.choice([10, 11, 12, 14, 15, 16, 17], p=[0.12, 0.16, 0.14, 0.14, 0.16, 0.16, 0.12])
        )
    elif bucket == 1:
        h = int(rng.choice([7, 8, 9, 13, 18, 19, 20, 21]))
    else:
        h = int(rng.choice([0, 1, 2, 3, 4, 5, 22, 23]))
    minute = int(rng.integers(0, 60))
    sec = int(rng.integers(0, 60))
    return time(h, minute, sec)


def _peso_dia_semana(dow: int) -> float:
    # 0..4 = seg-sex; 5 sab; 6 dom
    return {0: 1.0, 1: 1.05, 2: 1.05, 3: 1.05, 4: 1.10, 5: 0.6, 6: 0.35}[dow]


def _peso_dia_mes(d: int) -> float:
    if d in (5, 6):
        return 1.4  # folha
    if d in (29, 30, 31, 1):
        return 1.25  # aluguel/fim de mês
    return 1.0


def gerar_dataset(
    dias: int = 30,
    n_agencias: int = 50,
    n_associados: int = 10_000,
    n_operadores: int = 200,
    tx_por_dia_media: int = 5_000,
    seed: int = 42,
    com_fraudes: bool = True,
    output_path: Path | None = None,
) -> Path:
    """Gera dataset completo (dimensões + fato bronze) e salva em Parquet."""
    rng = np.random.default_rng(seed)

    dt_fim = date.today()
    dt_inicio = dt_fim - timedelta(days=dias - 1)

    log.info("dimensions.generating", n_agencias=n_agencias, n_associados=n_associados)
    dims = gerar_dimensoes(n_agencias, n_associados, n_operadores, dt_inicio, dt_fim, seed)

    agencia_ids = dims.agencias["agencia_id"].to_numpy()
    associado_ids = dims.associados["associado_id"].to_numpy()
    operador_ids = dims.operadores["operador_id"].to_numpy()

    all_rows: list[pd.DataFrame] = []

    for offset in range(dias):
        d = dt_inicio + timedelta(days=offset)
        dow = d.weekday()
        n_tx = int(tx_por_dia_media * _peso_dia_semana(dow) * _peso_dia_mes(d.day))
        n_tx = max(500, n_tx)

        tipos = rng.choice(TIPOS, size=n_tx, p=TIPO_PROBS)
        canais = rng.choice(CANAIS, size=n_tx, p=CANAL_PROBS)

        valores = np.empty(n_tx, dtype=np.float64)
        for t in np.unique(tipos):
            mask = tipos == t
            mu, sigma = VALOR_PARAMS[t]
            valores[mask] = rng.lognormal(mu, sigma, size=int(mask.sum())).round(2)

        ag = rng.choice(agencia_ids, size=n_tx)
        assoc = rng.choice(associado_ids, size=n_tx)
        # operador: só faz sentido em AGENCIA / ATM (parcial)
        op = np.where(
            np.isin(canais, ["AGENCIA", "ATM"]),
            rng.choice(operador_ids, size=n_tx),
            None,
        )

        # destino: apenas saídas precisam
        eh_saida = np.isin(tipos, ["PIX_OUT", "TED", "DOC"])
        destino = np.where(eh_saida, rng.choice(associado_ids, size=n_tx), None)

        horas = [_amostrar_hora(rng) for _ in range(n_tx)]
        ts = [datetime.combine(d, h) for h in horas]

        # geo: apenas canais digitais
        eh_digital = np.isin(canais, ["APP", "INTERNET_BANKING", "API"])
        # GO ~ centro do estado
        lat = np.where(eh_digital, rng.normal(-16.6, 1.5, size=n_tx), np.nan)
        lon = np.where(eh_digital, rng.normal(-49.3, 1.5, size=n_tx), np.nan)
        ip = np.where(eh_digital, [_fake_ip(rng) for _ in range(n_tx)], None)

        df = pd.DataFrame(
            {
                "transacao_id": [str(uuid.uuid4()) for _ in range(n_tx)],
                "tipo": tipos,
                "valor": valores,
                "dt_transacao": ts,
                "conta_origem": [_hash_conta(a) for a in assoc],
                "conta_destino": [(_hash_conta(d) if d is not None else None) for d in destino],
                "associado_id": assoc,
                "agencia_id": ag,
                "operador_id": op,
                "canal": canais,
                "ip_origem": ip,
                "geo_lat": lat,
                "geo_lon": lon,
                "dt_ingestao": pd.Timestamp.now(),
                "fraude_label": False,
                "fraude_tipo": None,
            }
        )
        all_rows.append(df)

    df_all = pd.concat(all_rows, ignore_index=True)
    log.info("transactions.generated", n=len(df_all))

    if com_fraudes:
        df_all = injetar_fraudes(df_all, dims, rng)
        log.info(
            "fraud.injected",
            n_fraudes=int(df_all["fraude_label"].sum()),
            tipos=df_all.loc[df_all["fraude_label"], "fraude_tipo"].value_counts().to_dict(),
        )

    output_path = output_path or (settings.bronze_path / "transacoes_raw")
    output_path.mkdir(parents=True, exist_ok=True)
    df_all["dt_partition"] = pd.to_datetime(df_all["dt_transacao"]).dt.date.astype(str)
    df_all.to_parquet(output_path, partition_cols=["dt_partition", "agencia_id"], index=False)

    # Salvar dimensões em silver (já curadas)
    silver = settings.silver_path
    silver.mkdir(parents=True, exist_ok=True)
    dims.agencias.to_parquet(silver / "dim_agencia.parquet", index=False)
    dims.associados.to_parquet(silver / "dim_associado.parquet", index=False)
    dims.operadores.to_parquet(silver / "dim_operador.parquet", index=False)
    dims.calendario.to_parquet(silver / "dim_calendario.parquet", index=False)

    log.info("dataset.persisted", path=str(output_path), n_rows=len(df_all))
    return output_path


def _hash_conta(associado_id: str | None) -> str | None:
    if associado_id is None:
        return None
    return f"CT{abs(hash(associado_id)) % 10**10:010d}"


def _fake_ip(rng: np.random.Generator) -> str:
    return ".".join(str(int(rng.integers(1, 254))) for _ in range(4))
