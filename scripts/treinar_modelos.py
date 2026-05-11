"""Treina Isolation Forest e Autoencoder a partir do silver."""

from __future__ import annotations

import click

from cacm.detectors.ml.autoencoder import train_autoencoder
from cacm.detectors.ml.isolation_forest import train_isolation_forest
from cacm.utils.logging import configure_logging


@click.command()
@click.option("--epochs", default=40, show_default=True)
def main(epochs: int) -> None:
    configure_logging("INFO")
    train_isolation_forest()
    train_autoencoder(epochs=epochs)


if __name__ == "__main__":
    main()
