# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Sprint 7 — Documentação e polish
- `docs/architecture.md`, `docs/detectors.md`, `docs/data-model.md`, `docs/operations.md`
- Post-mortem técnico em `docs/lessons-learned.md`
- `mkdocs.yml` consolidado

### Sprint 6 — Power BI
- Modelo dimensional documentado (`powerbi/data_model.md`)
- DAX measures + Power Query M template

### Sprint 5 — Scoring combinado
- Combinação ponderada dos detectores em `alerts/scoring.py`
- Persistência da tabela `alertas_auditoria` em gold

### Sprint 4 — Detectores ML
- Isolation Forest (M1) e Autoencoder (M2) com tracking MLflow
- Calibrador logístico (M3) sobre alertas rotulados

### Sprint 3 — Detectores estatísticos
- D1 CUSUM, D2 EWMA, D3 3-sigma, D4 Benford, D5 fracionamento (PLD)

### Sprint 2 — Pipeline bronze/silver
- Ingestão (batch + Structured Streaming) e camada silver enriquecida
- Camada gold de fatos agregados

### Sprint 1 — Gerador de dados sintéticos
- Gerador realista com 5 padrões de fraude rotulados (F1..F5)
- Dimensões (agências, associados, operadores, calendário)

### Sprint 0 — Setup
- Estrutura de repositório, Poetry, pre-commit (ruff/mypy), CI no GitHub Actions
- Pacote `cacm` com config (pydantic-settings), logging estruturado e SparkSession factory
- CLI `cacm` com subcomandos `gerar-dados`, `rodar-pipeline`, `treinar-modelos`
