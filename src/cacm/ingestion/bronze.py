"""Camada BRONZE: lê os Parquet crus gerados em `data/bronze/transacoes_raw/`.

A persistência inicial é feita pelo gerador (Faker → Parquet). Esta camada apenas
expõe um leitor padronizado para as etapas seguintes do pipeline.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from cacm.config import settings


def read_bronze(spark: SparkSession, path: Path | None = None) -> DataFrame:
    """Lê a tabela bronze de transações cruas."""
    p = path or (settings.bronze_path / "transacoes_raw")
    return spark.read.parquet(str(p))
