"""Camada GOLD: fatos analíticos agregados para dashboards.

A tabela de alertas é construída em `alerts.scoring`. Aqui ficam as agregações
para o cockpit Power BI.
"""

from __future__ import annotations

from pyspark.sql import functions as F

from cacm.config import settings
from cacm.utils.logging import get_logger
from cacm.utils.spark_session import get_spark

log = get_logger(__name__)


def build_gold() -> dict[str, str]:
    """Materializa fatos agregados em `data/gold/`."""
    spark = get_spark()
    silver = spark.read.parquet(str(settings.silver_path / "transacoes_silver"))

    paths: dict[str, str] = {}

    # 1. Fato por agência/dia
    f_ag_dia = (
        silver.groupBy("agencia_id", "dt")
        .agg(
            F.count("*").alias("qtd_tx"),
            F.sum("valor").alias("valor_total"),
            F.avg("valor").alias("valor_medio"),
            F.sum(F.when(F.col("flag_fora_horario"), 1).otherwise(0)).alias("qtd_fora_horario"),
            F.sum(F.when(F.col("flag_valor_alto"), 1).otherwise(0)).alias("qtd_valor_alto"),
        )
    )
    p = settings.gold_path / "fato_agencia_dia"
    f_ag_dia.write.mode("overwrite").parquet(str(p))
    paths["fato_agencia_dia"] = str(p)

    # 2. Fato por operador/dia
    f_op_dia = (
        silver.filter(F.col("operador_id").isNotNull())
        .groupBy("operador_id", "agencia_id", "dt")
        .agg(
            F.count("*").alias("qtd_tx"),
            F.sum("valor").alias("valor_total"),
            F.sum(F.when(F.col("flag_fora_horario"), 1).otherwise(0)).alias("qtd_fora_horario"),
        )
    )
    p = settings.gold_path / "fato_operador_dia"
    f_op_dia.write.mode("overwrite").parquet(str(p))
    paths["fato_operador_dia"] = str(p)

    # 3. Fato por associado/mês
    f_assoc_mes = (
        silver.withColumn("ano_mes", F.date_format("dt", "yyyy-MM"))
        .groupBy("associado_id", "ano_mes")
        .agg(
            F.count("*").alias("qtd_tx"),
            F.sum("valor").alias("valor_total"),
            F.avg("valor").alias("ticket_medio"),
            F.countDistinct("conta_destino").alias("destinos_unicos"),
            F.countDistinct("canal").alias("canais_distintos"),
        )
    )
    p = settings.gold_path / "fato_associado_mes"
    f_assoc_mes.write.mode("overwrite").parquet(str(p))
    paths["fato_associado_mes"] = str(p)

    log.info("gold.built", **paths)
    return paths
