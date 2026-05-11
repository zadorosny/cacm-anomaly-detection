# Detectores

Sete detectores no total (cinco estatísticos + dois ML + um calibrador supervisionado).

## Estatísticos

| ID | Nome | O que pega | Referência |
|---|---|---|---|
| D1 | CUSUM por agência | Mudança persistente de volume diário | Page (1954) |
| D2 | EWMA por associado (PIX) | Escalada gradual do ticket médio | Roberts (1959) |
| D3 | 3-sigma horário | Concentração anômala de transações em uma hora | Shewhart |
| D4 | Benford no primeiro dígito | Manipulação de valores (ajustes manuais, estornos) | Nigrini (2012) |
| D5 | Fracionamento (PLD) | Smurfing — múltiplas tx abaixo de R$ 10k somando >R$ 10k em 24h | Circular BACEN 3.978 |

## ML

| ID | Nome | O que pega | Referência |
|---|---|---|---|
| M1 | Isolation Forest | Padrão multivariado anômalo (combinação rara de features) | Liu, Ting, Zhou (2008) |
| M2 | Autoencoder denso | Perfil mensal do associado destoa do "normal" reconstruído | Hinton & Salakhutdinov (2006) |
| M3 | Calibrador logístico | Recalibra score combinado usando alertas confirmados/falsos | Cox (1958) |

## Combinação

Score final = média ponderada por detector (pesos default em `alerts.scoring.WEIGHTS_DEFAULT`).
Severidade derivada por bins:

| Bin | Severidade |
|---|---|
| `[0.50, 0.70)` | BAIXA |
| `[0.70, 0.85)` | MEDIA |
| `[0.85, 0.95)` | ALTA |
| `[0.95, 1.00]` | CRITICA |
| `< 0.50` | (descartado) |

## Calibração (CA/CM feedback loop)

À medida que o auditor classifica alertas em `CONFIRMADO` / `FALSO_POSITIVO`,
o calibrador M3 é retreinado periodicamente (`scripts/treinar_modelos.py`).
O score calibrado vira a probabilidade estimada de confirmação — usada para
ordenar a fila do cockpit (página 2).
