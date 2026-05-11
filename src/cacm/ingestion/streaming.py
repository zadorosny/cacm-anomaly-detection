"""Ingestão simulada via Spark Structured Streaming.

Lê arquivos JSON dropados em `data/landing/` (alimentados por um produtor externo)
e materializa para bronze em Parquet.

Uso de portfólio: demonstra Structured Streaming sem dependência de Kafka real.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from cacm.config import settings

SCHEMA = StructType(
    [
        StructField("transacao_id", StringType(), False),
        StructField("tipo", StringType(), False),
        StructField("valor", DoubleType(), False),
        StructField("dt_transacao", TimestampType(), False),
        StructField("conta_origem", StringType(), True),
        StructField("conta_destino", StringType(), True),
        StructField("associado_id", StringType(), False),
        StructField("agencia_id", StringType(), False),
        StructField("operador_id", StringType(), True),
        StructField("canal", StringType(), False),
        StructField("ip_origem", StringType(), True),
        StructField("geo_lat", DoubleType(), True),
        StructField("geo_lon", DoubleType(), True),
        StructField("dt_ingestao", TimestampType(), True),
        StructField("fraude_label", BooleanType(), True),
        StructField("fraude_tipo", StringType(), True),
    ]
)


def start_streaming_ingestion(
    spark: SparkSession,
    landing_path: Path | None = None,
    output_path: Path | None = None,
    checkpoint_path: Path | None = None,
) -> StreamingQuery:
    """Inicia o job streaming JSON → Parquet bronze."""
    landing = landing_path or (settings.data_root / "landing")
    output = output_path or (settings.bronze_path / "transacoes_streaming")
    checkpoint = checkpoint_path or (settings.checkpoints_path / "bronze_streaming")
    landing.mkdir(parents=True, exist_ok=True)

    df = (
        spark.readStream.schema(SCHEMA)
        .option("maxFilesPerTrigger", 10)
        .json(str(landing))
    )
    return (
        df.writeStream.format("parquet")
        .option("checkpointLocation", str(checkpoint))
        .partitionBy("agencia_id")
        .outputMode("append")
        .start(str(output))
    )
