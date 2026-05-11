# CA/CM Anomaly Detection — Continuous Auditing & Monitoring

> Pipeline de **Continuous Auditing / Continuous Monitoring** (CA/CM) com detecção de anomalias em transações de cooperativa de crédito. Projeto de portfólio que cobre **SQL, Python, Spark, Power BI, Git, Parquet, ML e estatística clássica**.

[![CI](https://github.com/zadorosny/cacm-anomaly-detection/actions/workflows/ci.yml/badge.svg)](https://github.com/zadorosny/cacm-anomaly-detection/actions/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/)
[![License](https://img.shields.io/badge/license-MIT-green)](./LICENSE)

📄 **Especificação completa:** [`01-continuous-auditing-monitoring.md`](./01-continuous-auditing-monitoring.md)

---

## Índice

1. [Visão geral](#visão-geral)
2. [Stack](#stack)
3. [Pré-requisitos](#pré-requisitos)
4. [Instalação](#instalação)
5. [Executando o pipeline](#executando-o-pipeline)
6. [Parâmetros configuráveis](#parâmetros-configuráveis)
7. [Inspecionando resultados](#inspecionando-resultados)
8. [Power BI](#power-bi)
9. [Testes e qualidade](#testes-e-qualidade)
10. [Estrutura do projeto](#estrutura-do-projeto)
11. [Troubleshooting](#troubleshooting)
12. [Documentação adicional](#documentação-adicional)

---

## Visão geral

```
                              ┌────────────────────────────┐
                              │  Faker (gerador sintético)  │
                              └──────────────┬──────────────┘
                                             ▼
                                ┌────────────────────────┐
                                │  BRONZE — Parquet cru  │
                                └──────────┬─────────────┘
                                           ▼
                                ┌────────────────────────┐
                                │  SILVER — limpo +      │
                                │  enriquecido (dims)    │
                                └──────────┬─────────────┘
                                           ▼
              ┌────────────────────────────┴────────────────────────────┐
              ▼                                                         ▼
   ┌──────────────────────┐                       ┌──────────────────────────────────────┐
   │  GOLD facts (agreg.) │                       │  Detectores                          │
   │  agência/dia,        │                       │   stat: CUSUM, EWMA, 3σ, Benford,    │
   │  operador/dia,       │                       │         fracionamento (PLD)           │
   │  associado/mês       │                       │   ML  : Isolation Forest, Autoencoder │
   └──────────┬───────────┘                       └────────────────────┬─────────────────┘
              │                                                        ▼
              │                                       ┌─────────────────────────────────┐
              │                                       │  GOLD — alertas_auditoria       │
              │                                       └────────────────────┬────────────┘
              └────────────────────────┬──────────────────────────────────┘
                                       ▼
                              ┌───────────────────┐
                              │  Power BI cockpit │
                              └───────────────────┘
```

Combina **estatística clássica auditável** (CUSUM, EWMA, 3-sigma, Benford, fracionamento PLD) com **ML não-supervisionado** (Isolation Forest, Autoencoder). A justificativa arquitetural — ML pega o que regras não pegam, estatística clássica é defensável em conselho — é deliberada.

## Stack

| Camada           | Tecnologia                                |
|------------------|-------------------------------------------|
| Linguagem        | Python 3.11+                              |
| Processamento    | Apache Spark 3.5 (PySpark)                |
| Storage          | Parquet (medallion bronze/silver/gold)    |
| Estatística      | scipy, statsmodels, numpy                 |
| ML               | scikit-learn (IF), PyTorch (Autoencoder)  |
| Tracking         | MLflow                                    |
| Visualização     | Power BI Desktop                          |
| Qualidade        | ruff, mypy, pytest, pre-commit            |
| CI               | GitHub Actions                            |
| Task runner      | `make` (Linux/macOS) ou `Run.bat` (Windows) |

---

## Pré-requisitos

| Ferramenta | Versão mínima | Notas                                   |
|------------|---------------|------------------------------------------|
| Python     | 3.11          | < 3.13 (pinado em `pyproject.toml`)      |
| Java       | 17 (JDK)      | PySpark exige JVM — Temurin/OpenJDK ok   |
| Poetry     | 1.8           | recomendado; pip funciona como fallback  |
| Power BI Desktop | 2024+    | apenas para abrir o cockpit (Windows)    |
| Make       | qualquer GNU make | só Linux/macOS; Windows usa `Run.bat` |
| Git LFS    | 3.x           | apenas se for versionar `.pbix`          |

Verifique rapidamente:

```bash
python --version    # 3.11.x ou 3.12.x
java -version       # 17.x
poetry --version
```

---

## Instalação

### Opção A — Poetry (recomendado)

```bash
git clone https://github.com/zadorosny/cacm-anomaly-detection.git
cd cacm-anomaly-detection

# Linux/macOS
make install

# Windows
Run.bat install
```

### Opção B — pip (sem Poetry)

```bash
git clone https://github.com/zadorosny/cacm-anomaly-detection.git
cd cacm-anomaly-detection
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements-dev.txt
```

Confirme a instalação:

```bash
poetry run cacm --help     # ou: python -m cacm.cli --help
```

---

## Executando o pipeline

O fluxo end-to-end tem três etapas: **gerar dados → treinar modelos → rodar pipeline**.

### Quick start (tudo de uma vez)

```bash
# Linux / macOS
make all

# Windows
Run.bat all
```

`make all` encadeia `install → data → train → pipeline` e termina com `data/gold/alertas_auditoria/` populado.

### Passo a passo

```bash
# 1) Gerar 30 dias de transações sintéticas (~150k linhas)
make data                # ou: Run.bat data

# 2) Treinar Isolation Forest + Autoencoder
make train               # ou: Run.bat train

# 3) Bronze → silver → gold → alertas
make pipeline            # ou: Run.bat pipeline
```

### Equivalente sem make

```bash
poetry run cacm gerar-dados --dias 30 --agencias 50
poetry run cacm treinar-modelos
poetry run cacm rodar-pipeline
```

ou pelos scripts diretos:

```bash
poetry run python scripts/gerar_dados.py --dias 30
poetry run python scripts/treinar_modelos.py --epochs 40
poetry run python scripts/rodar_pipeline.py
```

---

## Parâmetros configuráveis

Todos podem ser passados pela linha de comando do `make`/`Run.bat`:

| Variável     | Default | Descrição                                |
|--------------|---------|------------------------------------------|
| `DIAS`       | 30      | Dias de histórico a gerar                |
| `AGENCIAS`   | 50      | Quantas agências fictícias               |
| `ASSOCIADOS` | 10000   | Cadastro sintético de associados         |
| `OPERADORES` | 200     | Operadores (caixas/gerentes)             |
| `TX_POR_DIA` | 5000    | Volume médio diário de transações        |
| `SEED`       | 42      | Para reprodutibilidade                   |
| `EPOCHS`     | 40      | Épocas de treino do Autoencoder          |

Exemplos:

```bash
# 90 dias, 100 agências
make data DIAS=90 AGENCIAS=100

# Windows: variáveis de ambiente
set DIAS=90 && set AGENCIAS=100 && Run.bat data

# Equivalente direto (sem make)
poetry run cacm gerar-dados --dias 90 --agencias 100
```

Detector params (CUSUM `k`/`h`, EWMA `λ`/`L`, limites PLD, etc.) ficam em [`src/cacm/config.py`](./src/cacm/config.py) e podem ser sobrescritos por env vars com prefixo `CACM_`:

```bash
export CACM_CUSUM_H=4.0
export CACM_STRUCTURING_LIMIT_BRL=5000
make pipeline
```

---

## Inspecionando resultados

Após rodar o pipeline, os arquivos ficam em `data/`:

```
data/
├── bronze/
│   └── transacoes_raw/                  # parquet particionado dt/agencia
├── silver/
│   ├── transacoes_silver/               # parquet enriquecido com dims
│   ├── dim_agencia.parquet
│   ├── dim_associado.parquet
│   ├── dim_operador.parquet
│   └── dim_calendario.parquet
├── gold/
│   ├── alertas_auditoria/               # tabela final de alertas
│   ├── fato_agencia_dia/
│   ├── fato_operador_dia/
│   └── fato_associado_mes/
└── models/
    ├── isolation_forest.joblib
    ├── autoencoder.pt
    └── autoencoder_scaler.joblib
```

Inspeção rápida em Python:

```python
import pandas as pd
alertas = pd.read_parquet("data/gold/alertas_auditoria")
print(alertas[["severidade", "score", "motivo"]].head(20))
print(alertas["severidade"].value_counts())
```

MLflow UI (treino + métricas):

```bash
make mlflow      # http://localhost:5000
```

---

## Power BI

O cockpit (`powerbi/cacm.pbix`) consome diretamente os Parquets de `data/gold/`. Versionado via Git LFS.

```bash
git lfs install
```

Construção do `.pbix` (uma vez):

1. Rode `make all` para gerar `data/gold/`.
2. Abra o Power BI Desktop → **Get Data → Folder** → aponte para `data/gold/`.
3. Use a query M de [`powerbi/queries/power_query_alertas.m`](./powerbi/queries/power_query_alertas.m) como template.
4. Aplique relacionamentos e medidas DAX de [`powerbi/data_model.md`](./powerbi/data_model.md).
5. Monte as 4 páginas conforme [`powerbi/data_model.md`](./powerbi/data_model.md#4-páginas).
6. Salve como `powerbi/cacm.pbix`.

Refresh após cada rerun do pipeline: **Home → Refresh**.

---

## Testes e qualidade

```bash
make test               # unit (rápido)
make test-integration   # integração (requer Spark / Java 17)
make check              # roda tudo o que o CI roda: lint + format + typecheck + test
```

CI no GitHub Actions roda automaticamente a cada push/PR para `main` — ver [`.github/workflows/ci.yml`](./.github/workflows/ci.yml).

---

## Estrutura do projeto

```
cacm-anomaly-detection/
├── README.md                       # este arquivo
├── Makefile / Run.bat              # task runner Linux/Windows
├── requirements.txt                # deps fixadas (pip)
├── pyproject.toml                  # deps + ferramentas (Poetry)
├── 01-continuous-auditing-monitoring.md  # especificação completa
├── src/cacm/
│   ├── cli.py                      # CLI Click
│   ├── config.py                   # pydantic-settings
│   ├── generators/                 # Faker + injetor de fraudes
│   ├── ingestion/                  # bronze (batch + streaming)
│   ├── transformations/            # silver + gold
│   ├── detectors/
│   │   ├── statistical/            # CUSUM, EWMA, 3σ, Benford, fracionamento
│   │   └── ml/                     # Isolation Forest, Autoencoder, calibrator
│   ├── alerts/                     # scoring combinado + persistência
│   └── utils/                      # logging, SparkSession
├── tests/                          # pytest (unit + integration)
├── scripts/                        # entrypoints standalone
├── powerbi/                        # .pbix + modelo dimensional
├── docs/                           # mkdocs (architecture/detectors/...)
└── notebooks/                      # EDA + experimentação
```

---

## Troubleshooting

| Sintoma | Causa provável | Solução |
|---|---|---|
| `Py4JJavaError: NoClassDefFoundError` ao chamar Spark | `JAVA_HOME` ausente ou Java 8/11 | `export JAVA_HOME=$(/usr/libexec/java_home -v 17)` (macOS) ou apontar para JDK 17 |
| OOM no Spark local | dataset >2 GB com driver default | `export CACM_SPARK_DRIVER_MEMORY=4g` |
| `ModuleNotFoundError: cacm` | virtualenv não ativo | `poetry shell` ou ative o venv manualmente |
| Treino do Autoencoder muito lento | rodando em CPU | reduza `EPOCHS=10` ou habilite CUDA |
| `pyarrow` falha em Apple Silicon | wheel incorreto | `pip install --upgrade pyarrow` ou usar conda |
| pre-commit hook falha em `mypy` | dependências dev não instaladas | `make install` (não só `pip install -r requirements.txt`) |

---

## Documentação adicional

- [`docs/architecture.md`](./docs/architecture.md) — decisões arquiteturais, trade-offs
- [`docs/detectors.md`](./docs/detectors.md) — catálogo de detectores com referências
- [`docs/data-model.md`](./docs/data-model.md) — esquema bronze/silver/gold
- [`docs/operations.md`](./docs/operations.md) — runbook de operação
- [`docs/lessons-learned.md`](./docs/lessons-learned.md) — post-mortem técnico
- [`powerbi/data_model.md`](./powerbi/data_model.md) — modelo dimensional Power BI

Servir localmente:

```bash
make docs    # http://localhost:8000
```

---

## Licença

MIT — ver [`LICENSE`](./LICENSE).
