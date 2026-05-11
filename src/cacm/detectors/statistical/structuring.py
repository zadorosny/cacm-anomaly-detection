"""D5 — Detecção de fracionamento (PLD).

Regra: associado realiza N transações abaixo de `limite_brl` em janela de
`window_h` horas cuja **soma** ultrapassa o limite. Clássica em PLD/AML.
"""

from __future__ import annotations

import pandas as pd

from cacm.config import settings


def detectar_fracionamento(
    df_silver: pd.DataFrame,
    limite_brl: float | None = None,
    window_h: int | None = None,
    tipos: tuple[str, ...] = ("PIX_OUT", "TED"),
) -> pd.DataFrame:
    """Marca janelas suspeitas. Saída: 1 linha por janela detectada.

    `df_silver` precisa de: associado_id, dt_transacao, tipo, valor, transacao_id.
    """
    lim = settings.structuring_limit_brl if limite_brl is None else limite_brl
    w = settings.structuring_window_hours if window_h is None else window_h

    df = df_silver[df_silver["tipo"].isin(tipos)].copy()
    df["dt_transacao"] = pd.to_datetime(df["dt_transacao"])
    df = df[df["valor"] < lim]  # apenas tx abaixo do limite contam

    df = df.sort_values(["associado_id", "dt_transacao"]).reset_index(drop=True)
    out = []
    for assoc, g in df.groupby("associado_id"):
        g = g.set_index("dt_transacao")
        soma = g["valor"].rolling(f"{w}h").sum()
        cnt = g["valor"].rolling(f"{w}h").count()
        suspeita = (soma > lim) & (cnt >= 3)
        if suspeita.any():
            picos = g.loc[suspeita]
            # consolida pico final por associado/dia (1 alerta por dia)
            picos = picos.assign(dt=picos.index.date)
            agg = (
                picos.groupby("dt")
                .agg(qtd_tx=("valor", "size"), soma=("valor", "sum"))
                .reset_index()
            )
            agg["associado_id"] = assoc
            agg["regra_id"] = "D5_FRACIONAMENTO"
            agg["familia"] = "PLD"
            agg["alerta"] = True
            agg["score"] = (agg["soma"] / lim).clip(upper=2.0) / 2.0  # normaliza 0-1
            agg["motivo"] = agg.apply(
                lambda r: f"Fracionamento: {int(r.qtd_tx)} tx somando R$ {r.soma:,.2f} em {w}h", axis=1
            )
            out.append(agg)
    return pd.concat(out, ignore_index=True) if out else pd.DataFrame()
