"""Roda o pipeline completo: bronze -> silver -> gold + alertas."""

from __future__ import annotations

import click

from cacm.alerts.scoring import run_scoring_pipeline
from cacm.transformations.gold import build_gold
from cacm.transformations.silver import build_silver
from cacm.utils.logging import configure_logging


@click.command()
def main() -> None:
    configure_logging("INFO")
    build_silver()
    build_gold()
    run_scoring_pipeline()


if __name__ == "__main__":
    main()
