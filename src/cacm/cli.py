"""CLI principal do pipeline (entry-point `cacm`)."""

from __future__ import annotations

import click

from cacm import __version__
from cacm.utils.logging import configure_logging, get_logger

log = get_logger(__name__)


@click.group()
@click.version_option(__version__)
@click.option("--log-level", default="INFO", show_default=True)
def main(log_level: str) -> None:
    """CA/CM — pipeline de auditoria contínua."""
    configure_logging(log_level)


@main.command()
@click.option("--dias", default=30, show_default=True, help="Dias de histórico a gerar.")
@click.option("--agencias", default=50, show_default=True)
@click.option("--associados", default=10_000, show_default=True)
def gerar_dados(dias: int, agencias: int, associados: int) -> None:
    """Gera dados sintéticos de transações e dimensões."""
    from cacm.generators.transacoes import gerar_dataset

    log.info("starting.synthetic_data", dias=dias, agencias=agencias, associados=associados)
    gerar_dataset(dias=dias, n_agencias=agencias, n_associados=associados)


@main.command()
def rodar_pipeline() -> None:
    """Roda bronze → silver → gold (detectores + alertas)."""
    from cacm.alerts.scoring import run_scoring_pipeline
    from cacm.transformations.gold import build_gold
    from cacm.transformations.silver import build_silver

    log.info("pipeline.start")
    build_silver()
    build_gold()
    run_scoring_pipeline()
    log.info("pipeline.done")


@main.command()
def treinar_modelos() -> None:
    """Treina Isolation Forest e Autoencoder sobre silver."""
    from cacm.detectors.ml.isolation_forest import train_isolation_forest
    from cacm.detectors.ml.autoencoder import train_autoencoder

    log.info("training.start")
    train_isolation_forest()
    train_autoencoder()
    log.info("training.done")


if __name__ == "__main__":
    main()
