# Modelo de dados

## Bronze — `transacoes_raw`

Tabela cruda, esquema fiel à fonte. Particionada por `dt_partition × agencia_id`.

| Campo | Tipo | Descrição |
|---|---|---|
| `transacao_id` | string (UUID) | PK |
| `tipo` | string | PIX_OUT, PIX_IN, TED, DOC, SAQUE, DEPOSITO |
| `valor` | double | BRL |
| `dt_transacao` | timestamp | timezone America/Sao_Paulo |
| `conta_origem` | string | hash |
| `conta_destino` | string\|null | hash |
| `associado_id` | string | hash |
| `agencia_id` | string | FK dim_agencia |
| `operador_id` | string\|null | FK dim_operador |
| `canal` | string | APP, INTERNET_BANKING, ATM, AGENCIA, API |
| `ip_origem` | string\|null | digital |
| `geo_lat`, `geo_lon` | double | digital |
| `dt_ingestao` | timestamp | |
| `fraude_label` | bool | ground truth (sintético) |
| `fraude_tipo` | string\|null | F1..F5 |

## Silver — `transacoes_silver`

Resultado de `transformations.silver.build_silver`. Adiciona:
- Junção com `dim_associado`, `dim_agencia`, `dim_calendario`
- Colunas derivadas: `hora`, `dia_semana`, `primeiro_digito`
- Flags determinísticas: `flag_fora_horario`, `flag_fim_de_semana`, `flag_valor_alto`, `flag_alta_alcada`

## Dimensões (silver)

- `dim_agencia`: agencia_id, municipio, uf, central, porte, endereco
- `dim_associado`: associado_id, segmento, dt_adesao, score_risco, uf_residencia
- `dim_operador`: operador_id, funcao, dt_admissao, agencia_lotacao
- `dim_calendario`: dt, ano, mes, dia_semana, dia_util, feriado

## Gold

- `fato_agencia_dia` — qtd_tx, valor_total, valor_medio, qtd_fora_horario, qtd_valor_alto
- `fato_operador_dia` — qtd_tx, valor_total, qtd_fora_horario
- `fato_associado_mes` — qtd_tx, valor_total, ticket_medio, destinos_unicos, canais_distintos
- `alertas_auditoria` — schema da seção 5.3 do spec

## Convenções
- Datas: timezone `America/Sao_Paulo` em tudo.
- IDs hashed: gerados via `abs(hash(...))` (em produção, salt + HMAC-SHA256).
- Tipo monetário: `double` no Spark (limitação Parquet com decimal em PySpark); arredondado para 2 casas.
