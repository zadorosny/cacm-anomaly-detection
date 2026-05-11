"""Configurações globais do pipeline.

Usa pydantic-settings para permitir override via variáveis de ambiente
(prefixo `CACM_`).
"""

from __future__ import annotations

from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

REPO_ROOT = Path(__file__).resolve().parents[2]


class Settings(BaseSettings):
    """Configurações do pipeline CA/CM."""

    model_config = SettingsConfigDict(
        env_prefix="CACM_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    data_root: Path = Field(default=REPO_ROOT / "data")

    @property
    def bronze_path(self) -> Path:
        return self.data_root / "bronze"

    @property
    def silver_path(self) -> Path:
        return self.data_root / "silver"

    @property
    def gold_path(self) -> Path:
        return self.data_root / "gold"

    @property
    def checkpoints_path(self) -> Path:
        return self.data_root / "checkpoints"

    spark_app_name: str = "cacm"
    spark_master: str = "local[*]"
    spark_shuffle_partitions: int = 8

    # Detector params
    cusum_k: float = 0.5  # reference value (slack)
    cusum_h: float = 5.0  # decision threshold (in sigmas)
    ewma_lambda: float = 0.2
    ewma_L: float = 3.0
    three_sigma_k: float = 3.0

    # PLD / fracionamento
    structuring_limit_brl: float = 10_000.00
    structuring_window_hours: int = 24

    # Scoring bins
    score_bin_baixa: float = 0.5
    score_bin_media: float = 0.7
    score_bin_alta: float = 0.85
    score_bin_critica: float = 0.95


settings = Settings()
