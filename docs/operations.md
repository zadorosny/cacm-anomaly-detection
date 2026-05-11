# Operações

## Pré-requisitos

- Python 3.11+
- Java 17 (PySpark exige JVM)
- Poetry 1.8+
- (Opcional) Power BI Desktop para o `.pbix`

## Setup

```bash
poetry install
poetry run pre-commit install
```

## Fluxo end-to-end

```bash
# 1. Gerar 30 dias de dados sintéticos
poetry run cacm gerar-dados --dias 30 --agencias 50

# 2. Treinar modelos ML
poetry run cacm treinar-modelos

# 3. Rodar pipeline completo (bronze -> silver -> gold + alertas)
poetry run cacm rodar-pipeline

# 4. Inspecionar alertas
poetry run python -c "import pandas as pd; print(pd.read_parquet('data/gold/alertas_auditoria').head(20))"

# 5. Abrir cockpit
# powerbi/cacm.pbix
```

## Testes

```bash
poetry run pytest                       # unit
poetry run pytest -m integration        # requer Spark local
poetry run pytest -m "not slow"         # exclui treino de modelos
```

## Lint / type-check

```bash
poetry run ruff check src tests
poetry run ruff format src tests
poetry run mypy src
```

## MLflow UI

```bash
poetry run mlflow ui --port 5000
# http://localhost:5000
```

## Troubleshooting

- **`Py4JJavaError: ... NoClassDefFoundError`**: garantir `JAVA_HOME` apontando para Java 17.
- **OOM no Spark local**: aumentar `spark.driver.memory` em `utils/spark_session.py` ou exportar `CACM_SPARK_DRIVER_MEMORY=4g`.
- **Treino de Autoencoder lento na CPU**: reduzir `--epochs` ou rodar em GPU (CUDA disponível → PyTorch detecta automaticamente).
