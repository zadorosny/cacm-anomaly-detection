"""Persistência da tabela `alertas_auditoria` na camada gold."""

from __future__ import annotations

import pandas as pd

from cacm.config import settings


def save_alertas(df: pd.DataFrame) -> str:
    """Salva (overwrite) a tabela gold de alertas."""
    out = settings.gold_path / "alertas_auditoria"
    out.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out, index=False)
    return str(out)
