# CA/CM Anomaly Detection — Continuous Auditing & Monitoring

> Pipeline de **Continuous Auditing / Continuous Monitoring** (CA/CM) com detecção de anomalias em transações de cooperativa de crédito. Projeto de portfólio que cobre SQL, Python, Spark, Power BI, Git, Parquet, ML e estatística clássica.

📄 **Especificação completa:** [`01-continuous-auditing-monitoring.md`](./01-continuous-auditing-monitoring.md)

## Visão geral

```
Faker → Spark Streaming → BRONZE → SILVER → Detectores (Stat + ML) → GOLD (alertas) → Power BI
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

## Como rodar

```bash
# 1. Clonar e instalar
git clone https://github.com/zadorosny/cacm-anomaly-detection.git
cd cacm-anomaly-detection
poetry install
poetry run pre-commit install

# 2. Gerar dados sintéticos (30 dias, 50 agências, 10k associados)
poetry run cacm gerar-dados --dias 30 --agencias 50

# 3. Rodar pipeline completo (bronze → silver → gold + alertas)
poetry run cacm rodar-pipeline

# 4. Treinar modelos ML
poetry run cacm treinar-modelos

# 5. Abrir cockpit Power BI
# powerbi/cacm.pbix
```

## Estrutura

Ver seção 8 da [especificação](./01-continuous-auditing-monitoring.md#8-estrutura-do-repositório).

## Testes

```bash
poetry run pytest                # unit
poetry run pytest -m integration # integration (requer Spark local)
```

## Status

Em construção — ver [`CHANGELOG.md`](./CHANGELOG.md) para progresso por sprint.

## Licença

MIT — ver [`LICENSE`](./LICENSE).
