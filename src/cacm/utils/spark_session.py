"""Fábrica de SparkSession para o pipeline."""

from __future__ import annotations

from pyspark.sql import SparkSession

from cacm.config import settings


def get_spark(app_name: str | None = None) -> SparkSession:
    """Retorna SparkSession local com configurações padrão do projeto."""
    builder = (
        SparkSession.builder.appName(app_name or settings.spark_app_name)
        .master(settings.spark_master)
        .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
        .config("spark.sql.session.timeZone", "America/Sao_Paulo")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.showConsoleProgress", "false")
    )
    return builder.getOrCreate()
