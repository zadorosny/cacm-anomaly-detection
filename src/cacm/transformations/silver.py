"""Camada SILVER: limpeza, tipagem, deduplicação e enriquecimento.

Regras determinísticas de auditoria são aplicadas aqui (flags), mas o disparo
de alertas fica na camada gold.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from cacm.config import settings
from cacm.ingestion.bronze import read_bronze
from cacm.utils.logging import get_logger
from cacm.utils.spark_session import get_spark

log = get_logger(__name__)


def clean_transactions(df: DataFrame) -> DataFrame:
    """Limpeza/tipagem mínima."""
    return (
        df.filter(F.col("transacao_id").isNotNull())
        .filter(F.col("valor") > 0)
        .filter(F.col("dt_transacao").isNotNull())
        .dropDuplicates(["transacao_id"])
    )


def enrich_transactions(spark: SparkSession, tx: DataFrame) -> DataFrame:
    """Enriquece com dimensões e deriva flags de auditoria determinísticas."""
    silver = settings.silver_path
    dim_associado = spark.read.parquet(str(silver / "dim_associado.parquet"))
    dim_agencia = spark.read.parquet(str(silver / "dim_agencia.parquet"))
    dim_calendario = spark.read.parquet(str(silver / "dim_calendario.parquet"))

    tx = (
        tx.withColumn("dt", F.to_date("dt_transacao"))
        .withColumn("hora", F.hour("dt_transacao"))
        .withColumn("dia_semana", F.dayofweek("dt_transacao") - 1)  # 0=segunda
        .withColumn("primeiro_digito", F.substring(F.regexp_replace(F.col("valor").cast("string"), "[^1-9]", ""), 1, 1).cast("int"))
    )
    tx = tx.join(dim_associado, on="associado_id", how="left") \
           .join(dim_agencia, on="agencia_id", how="left") \
           .join(dim_calendario, on="dt", how="left")

    # Flags determinísticas de auditoria
    tx = (
        tx.withColumn("flag_fora_horario", (F.col("hora") < 6) | (F.col("hora") >= 22))
        .withColumn("flag_fim_de_semana", ~F.coalesce(F.col("dia_util"), F.lit(True)))
        .withColumn("flag_valor_alto", F.col("valor") > 50_000)
        .withColumn(
            "flag_alta_alcada",
            (F.col("valor") > 100_000) & (F.col("canal") != F.lit("AGENCIA")),
        )
    )
    return tx


def build_silver(output_name: str = "transacoes_silver") -> str:
    """Lê bronze, limpa, enriquece e persiste como silver Parquet."""
    spark = get_spark()
    bronze = read_bronze(spark)
    log.info("silver.input_rows", n=bronze.count())

    clean = clean_transactions(bronze)
    enriched = enrich_transactions(spark, clean)

    out = settings.silver_path / output_name
    (
        enriched.write.mode("overwrite")
        .partitionBy("dt", "agencia_id")
        .parquet(str(out))
    )
    log.info("silver.persisted", path=str(out))
    return str(out)
