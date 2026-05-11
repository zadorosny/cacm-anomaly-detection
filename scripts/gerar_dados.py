"""Script de conveniência para gerar dados sintéticos.

Uso:
    poetry run python scripts/gerar_dados.py --dias 30 --agencias 50
"""

from __future__ import annotations

import click

from cacm.generators.transacoes import gerar_dataset
from cacm.utils.logging import configure_logging


@click.command()
@click.option("--dias", default=30, show_default=True)
@click.option("--agencias", default=50, show_default=True)
@click.option("--associados", default=10_000, show_default=True)
@click.option("--operadores", default=200, show_default=True)
@click.option("--tx-por-dia", default=5_000, show_default=True)
@click.option("--seed", default=42, show_default=True)
def main(dias: int, agencias: int, associados: int, operadores: int, tx_por_dia: int, seed: int) -> None:
    configure_logging("INFO")
    gerar_dataset(
        dias=dias, n_agencias=agencias, n_associados=associados,
        n_operadores=operadores, tx_por_dia_media=tx_por_dia, seed=seed,
    )


if __name__ == "__main__":
    main()
