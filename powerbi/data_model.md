# Modelo Power BI — CA/CM

Arquivo: `cacm.pbix` (binário, versionado via Git LFS — veja `.gitattributes`).

## 1. Fontes de dados

Power BI consome diretamente os Parquets da camada **gold** via conector "Folder" + transformação Parquet (Power Query). Em produção, substituir por SQL Server / Delta Lake.

| Tabela | Caminho | Granularidade |
|---|---|---|
| `alertas_auditoria` | `data/gold/alertas_auditoria/` | 1 linha por alerta |
| `fato_agencia_dia` | `data/gold/fato_agencia_dia/` | agência × dia |
| `fato_operador_dia` | `data/gold/fato_operador_dia/` | operador × agência × dia |
| `fato_associado_mes` | `data/gold/fato_associado_mes/` | associado × mês |
| `dim_agencia` | `data/silver/dim_agencia.parquet` | dimensão |
| `dim_associado` | `data/silver/dim_associado.parquet` | dimensão |
| `dim_operador` | `data/silver/dim_operador.parquet` | dimensão |
| `dim_calendario` | `data/silver/dim_calendario.parquet` | dimensão |

## 2. Relacionamentos (estrela)

```
                ┌──────────────────┐
                │ dim_calendario   │ (dt)
                └────────┬─────────┘
                         │ 1:N
                         ▼
fato_agencia_dia ────────┤
fato_operador_dia ───────┤
                         │
            ┌────────────┴────────────┐
            ▼                         ▼
   dim_agencia (agencia_id)    dim_operador (operador_id)

            alertas_auditoria.transacao_id  ──(opcional)─→  silver.transacoes
```

Cardinalidades: todas N:1 partindo dos fatos para as dimensões.

## 3. Medidas DAX recomendadas

```DAX
-- KPIs do mês corrente
Alertas Abertos = CALCULATE(COUNTROWS(alertas_auditoria), alertas_auditoria[status] = "ABERTO")
Alertas Críticos = CALCULATE(COUNTROWS(alertas_auditoria), alertas_auditoria[severidade] = "CRITICA")
% Falso Positivo =
DIVIDE(
    CALCULATE(COUNTROWS(alertas_auditoria), alertas_auditoria[status] = "FALSO_POSITIVO"),
    CALCULATE(COUNTROWS(alertas_auditoria), NOT(ISBLANK(alertas_auditoria[status])))
)
Score Médio = AVERAGE(alertas_auditoria[score])
Tempo Médio Tratamento (dias) = AVERAGEX(
    FILTER(alertas_auditoria, alertas_auditoria[status] <> "ABERTO"),
    DATEDIFF(alertas_auditoria[dt_alerta], TODAY(), DAY)
)

-- Por família
Alertas por Família = COUNTROWS(alertas_auditoria)

-- Normalização (alertas / 1k transações na agência)
Alertas Normalizados = DIVIDE([Alertas Abertos], SUM(fato_agencia_dia[qtd_tx]) / 1000)
```

## 4. Páginas

### Página 1 — Visão Executiva
- Card: Alertas Críticos, % Falso Positivo, Score Médio, Tempo Médio Tratamento
- Mapa (coroplético) Goiás por município com `Alertas por Família`
- Gráfico de linha mensal por família de risco (FRAUDE / PLD / CONTROLE_INTERNO)
- Tabela: Top 10 agências por `Alertas Normalizados`

### Página 2 — Cockpit Operacional
- Slicers: `dim_agencia[municipio]`, `alertas_auditoria[familia]`, `dim_calendario[mes]`, `alertas_auditoria[canal]` (medido), `alertas_auditoria[severidade]`
- Tabela: alertas abertos com severidade, score, motivo, agência, dt_alerta
- Drill-through para detalhe da transação (id, valor, canal, geo)

### Página 3 — Análise Estatística
- Gráfico de linhas: série CUSUM (S_h, S_l) por agência selecionada
- Histograma de primeiro dígito com sobreposição da curva Benford esperada
- Card: chi², p-value, MAD
- Histograma de scores ML com linha vertical no limiar (p99)

### Página 4 — Performance dos Modelos
- Matriz de confusão (`status` × `score >= 0.85`)
- Taxa de falso positivo por `regra_id`
- PSI ao longo do tempo (calculado em pipeline, exposto em tabela)

## 5. Como atualizar

1. Rodar `poetry run cacm rodar-pipeline` para regenerar gold.
2. No Power BI Desktop: **Home → Refresh**.
3. Em produção, agendar refresh via Power BI Service (gateway local apontando para `data/gold/`).
