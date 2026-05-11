"""Combinação ponderada dos detectores e materialização da tabela `alertas_auditoria`.

Score final = média ponderada por detector. Severidade derivada por bins do score.
"""

from __future__ import annotations

import uuid

import numpy as np
import pandas as pd

from cacm.alerts.persistence import save_alertas
from cacm.config import settings
from cacm.detectors.statistical.benford import benford_por_segmento
from cacm.detectors.statistical.cusum import cusum_por_agencia
from cacm.detectors.statistical.ewma import ewma_por_associado_pix
from cacm.detectors.statistical.structuring import detectar_fracionamento
from cacm.detectors.statistical.three_sigma import three_sigma_concentracao_horaria
from cacm.utils.logging import get_logger

log = get_logger(__name__)

# Pesos default — em produção viriam de calibração ROC
WEIGHTS_DEFAULT = {
    "D1_CUSUM": 0.10,
    "D2_EWMA": 0.10,
    "D3_3SIGMA_HORA": 0.08,
    "D4_BENFORD": 0.07,
    "D5_FRACIONAMENTO": 0.20,
    "M1_ISOLATION_FOREST": 0.25,
    "M2_AUTOENCODER": 0.20,
}


def severidade_por_score(score: float) -> str | None:
    if score < settings.score_bin_baixa:
        return None  # ignorado
    if score < settings.score_bin_media:
        return "BAIXA"
    if score < settings.score_bin_alta:
        return "MEDIA"
    if score < settings.score_bin_critica:
        return "ALTA"
    return "CRITICA"


def combinar_scores(
    *detector_outputs: pd.DataFrame, weights: dict[str, float] | None = None
) -> pd.DataFrame:
    """Junta todos os outputs de detectores por transacao_id e calcula score final.

    Cada DataFrame deve ter: `regra_id` e identificador da entidade (`transacao_id`
    para fato, ou `associado_id`/`agencia_id`). Para esta versão simplificada,
    consolidamos por *entidade primária* (transacao_id quando disponível, fallback
    para associado_id).
    """
    w = weights or WEIGHTS_DEFAULT
    pieces = []
    for df in detector_outputs:
        if df is None or df.empty:
            continue
        d = df.copy()
        entity = (
            d["transacao_id"]
            if "transacao_id" in d.columns
            else d.get("associado_id", d.get("agencia_id", pd.Series(["-"] * len(d))))
        )
        d = d.assign(entidade=entity.astype(str))
        pieces.append(d[["entidade", "regra_id", "familia", "score", "alerta", "motivo"]])

    if not pieces:
        return pd.DataFrame()

    long = pd.concat(pieces, ignore_index=True)
    long["weight"] = long["regra_id"].map(w).fillna(0.0)
    long["score_w"] = long["score"].astype(float) * long["weight"]

    agg = (
        long.groupby("entidade")
        .agg(
            score=("score_w", "sum"),
            peso_total=("weight", "sum"),
            familias=("familia", lambda s: "|".join(sorted(set(s)))),
            regras=("regra_id", lambda s: "|".join(sorted(set(s)))),
            motivos=("motivo", lambda s: " | ".join(sorted({m for m in s if isinstance(m, str)}))),
        )
        .reset_index()
    )
    agg["score"] = (agg["score"] / agg["peso_total"].replace(0, np.nan)).fillna(0.0).clip(0, 1)
    agg["severidade"] = agg["score"].map(severidade_por_score)
    agg = agg[agg["severidade"].notna()].copy()
    agg["familia"] = agg["familias"].str.split("|").str[0]
    return agg


def montar_alertas(agg: pd.DataFrame) -> pd.DataFrame:
    """Converte agregação no schema final de `alertas_auditoria`."""
    return pd.DataFrame(
        {
            "alerta_id": [str(uuid.uuid4()) for _ in range(len(agg))],
            "transacao_id": agg["entidade"].to_numpy(),
            "dt_alerta": pd.Timestamp.now(),
            "regra_id": agg["regras"].to_numpy(),
            "familia": agg["familia"].to_numpy(),
            "severidade": agg["severidade"].to_numpy(),
            "score": agg["score"].to_numpy(),
            "motivo": agg["motivos"].to_numpy(),
            "status": "ABERTO",
        }
    )


def run_scoring_pipeline() -> str:
    """Lê silver, executa todos os detectores e materializa `alertas_auditoria`.

    Esta função orquestra a execução. Detectores ML são opcionais — se modelos
    não estiverem treinados, são pulados.
    """
    from cacm.utils.spark_session import get_spark

    spark = get_spark("scoring")
    silver = spark.read.parquet(str(settings.silver_path / "transacoes_silver")).toPandas()
    log.info("scoring.input", n=len(silver))

    # Estatísticos
    pix = silver[silver["tipo"].str.startswith("PIX")].copy()
    ag_dia = silver.groupby(["agencia_id", "dt"]).size().reset_index(name="qtd_tx")

    outputs = [
        cusum_por_agencia(ag_dia),
        ewma_por_associado_pix(pix[["associado_id", "dt", "valor"]]),
        three_sigma_concentracao_horaria(silver[["agencia_id", "dt_transacao"]]),
        benford_por_segmento(silver[["tipo", "valor"]], key="tipo"),
        detectar_fracionamento(
            silver[["associado_id", "dt_transacao", "tipo", "valor", "transacao_id"]]
        ),
    ]

    # ML — só se modelos existirem
    models_dir = settings.data_root / "models"
    if (models_dir / "isolation_forest.joblib").exists():
        from cacm.detectors.ml.isolation_forest import score_isolation_forest

        outputs.append(score_isolation_forest(silver))
    if (models_dir / "autoencoder.pt").exists():
        from cacm.detectors.ml.autoencoder import score_autoencoder

        outputs.append(score_autoencoder(silver))

    agg = combinar_scores(*outputs)
    alertas = montar_alertas(agg)
    log.info(
        "alertas.gerados",
        n=len(alertas),
        distribuicao=alertas["severidade"].value_counts().to_dict(),
    )

    return save_alertas(alertas)
