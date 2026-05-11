# Projeto 1 — Continuous Auditing & Monitoring (CA/CM) com Detecção de Anomalias em Transações de Cooperativa de Crédito

> **Contexto de portfólio:** projeto desenhado para a vaga de Analista de Dados — Auditoria Interna em uma cooperativa de crédito regional (Goiânia/GO). Cobre integralmente a stack pedida (SQL, Python, Spark, Power BI, Git, Parquet) e os desejáveis (IA, ML, Estatística), enquadrados no paradigma de Continuous Auditing / Continuous Monitoring (CA/CM) do IIA — Institute of Internal Auditors.

---

## 1. Resumo executivo

Pipeline de monitoramento contínuo que processa transações financeiras de uma cooperativa de crédito (PIX, TED, DOC, saques, depósitos) e gera **alertas auditáveis** quando detecta padrões anômalos: possível fraude interna, lavagem de dinheiro, falha de controle interno, ou desvio operacional. O resultado é consumido em um cockpit Power BI pelo time de auditoria interna, com drill-down até a transação individual.

O projeto combina **dois paradigmas complementares** para detecção:

1. **Estatística clássica e auditável** — cartas de controle (CUSUM, EWMA), limites 3-sigma, testes de hipótese. Interpretável, defensável em conselho, alinhada com práticas tradicionais de auditoria.
2. **Machine Learning não-supervisionado** — Isolation Forest e Autoencoder para padrões multivariados que regras não capturam.

A decisão arquitetural de combinar os dois é deliberada e é um dos pontos a destacar na entrevista: **ML pega o que regras não pegam, mas estatística clássica é auditável e defensável**. Auditoria interna precisa dos dois.

---

## 2. Por que esse projeto fala com a vaga

Mapeamento direto requisito → entregável do projeto:

| Requisito da vaga | Como o projeto cobre |
|---|---|
| SQL | Spark SQL pesado nas camadas silver/gold: window functions, agregações por janela, joins entre fatos e dimensões |
| Python | Orquestração completa, testes com pytest, type hints, estrutura de pacote |
| Spark | Structured Streaming consumindo de fonte simulada, transformações distribuídas, MLlib para baseline |
| Power BI | Cockpit de auditoria interativo com mapa, ranking, drill-down e semáforo de risco |
| Git | Repositório com branches, commits semânticos (Conventional Commits), CI no GitHub Actions, README e CHANGELOG |
| Parquet | Storage primário em arquitetura medallion (bronze/silver/gold), particionamento por data/agência |
| IA/ML (desejável) | Isolation Forest (scikit-learn) + Autoencoder (PyTorch ou Keras) para anomalia |
| Estatística (desejável) | CUSUM, EWMA, 3-sigma, teste qui-quadrado, intervalo de confiança em KPIs |
| Inglês (mandatório leitura) | Documentação técnica em inglês, referências bibliográficas em inglês |

---

## 3. Contexto de negócio (a história a contar na entrevista)

Cooperativas de crédito de grande porte operam milhares de agências e processa volumes massivos de transações diariamente. A auditoria interna não consegue mais operar no modelo tradicional de **amostragem aleatória anual** — o volume é alto demais e a velocidade dos riscos é maior que o ciclo de auditoria.

A resposta moderna a esse problema é **Continuous Auditing / Continuous Monitoring (CA/CM)**, conceito formalizado pelo IIA e amplamente adotado em instituições financeiras reguladas. Em CA/CM:

- A população **inteira** de transações é analisada, não amostras
- A análise roda **continuamente**, não em ciclos
- Indicadores de risco são monitorados em **tempo quase real**
- Alertas direcionam o auditor a investigar **somente as exceções**

Esse projeto é uma implementação simplificada e de portfólio de um sistema CA/CM, focado em três famílias de risco que aparecem em qualquer cooperativa de crédito:

1. **Fraude transacional** — comportamento atípico de associado ou operador
2. **Indícios de lavagem (PLD/AML)** — fracionamento, smurfing, atipicidade de fluxo
3. **Falha de controle interno** — transações fora de horário, fora de alçada, segregação de funções

---

## 4. Arquitetura

### 4.1 Visão geral

```
┌─────────────────────┐
│ Gerador sintético   │  Python + Faker
│ de transações       │  (simula fluxo real)
└──────────┬──────────┘
           │ JSON / Kafka local (ou socket)
           ▼
┌─────────────────────┐
│ Spark Structured    │  Ingestão streaming
│ Streaming           │
│ (BRONZE)            │
└──────────┬──────────┘
           │ Parquet bruto, particionado por dt
           ▼
┌─────────────────────┐
│ Spark Batch         │  Limpeza, tipagem,
│ (SILVER)            │  enriquecimento, dedup
└──────────┬──────────┘
           │ Parquet curado
           ▼
┌─────────────────────┐
│ Detecção            │  Stat (CUSUM/EWMA) +
│ (GOLD)              │  ML (Isolation Forest +
│                     │       Autoencoder)
└──────────┬──────────┘
           │ Tabela de alertas + KPIs agregados
           ▼
┌─────────────────────┐
│ Power BI            │  Cockpit de auditoria
└─────────────────────┘
```

### 4.2 Camadas (medallion)

**Bronze** — dados crus, esquema "as-is" da fonte. Parquet particionado por `dt_ingestao` e `agencia_id`. Sem tratamento. Auditável: tudo que entrou está aqui.

**Silver** — limpos, tipados, deduplicados, enriquecidos com dimensões (cadastro de associado, cadastro de agência, calendário). Esquema rígido validado. Aqui ficam as regras determinísticas de auditoria (transação fora de horário, valor zerado, conta encerrada).

**Gold** — duas famílias de tabelas:
1. **Fatos analíticos agregados** para dashboards (por agência/dia, por operador/dia, por associado/mês)
2. **Tabela de alertas** com score, motivo, severidade, link para transação original

### 4.3 Stack técnica

- **Orquestração:** Python 3.11+, Poetry para dependências
- **Processamento:** Apache Spark 3.5 (PySpark) com Structured Streaming
- **Storage:** Parquet (com possibilidade de upgrade para Delta Lake)
- **ML:** scikit-learn (Isolation Forest), PyTorch (Autoencoder), MLflow para tracking
- **Estatística:** scipy.stats, numpy, statsmodels
- **Visualização:** Power BI Desktop (.pbix versionado no Git)
- **Qualidade:** pytest, ruff, mypy, pre-commit
- **CI:** GitHub Actions (lint + test + build)
- **Documentação:** MkDocs Material

---

## 5. Modelo de dados

### 5.1 Tabela bronze: `transacoes_raw`

| Campo | Tipo | Descrição |
|---|---|---|
| transacao_id | string (UUID) | Identificador único |
| tipo | string | PIX_OUT, PIX_IN, TED, DOC, SAQUE, DEPOSITO |
| valor | decimal(18,2) | Valor em BRL |
| dt_transacao | timestamp | Timestamp completo |
| conta_origem | string | Hash da conta |
| conta_destino | string | Hash da conta (quando aplicável) |
| associado_id | string | Hash do associado titular |
| agencia_id | string | Código da agência |
| operador_id | string | Operador que processou (quando aplicável) |
| canal | string | APP, INTERNET_BANKING, ATM, AGENCIA, API |
| ip_origem | string | Quando canal digital |
| geo_lat / geo_lon | float | Quando disponível |
| dt_ingestao | timestamp | Quando entrou no pipeline |

### 5.2 Dimensões (silver)

- `dim_associado` — perfil, segmento, data de adesão, score de risco
- `dim_agencia` — município, UF, central, porte
- `dim_operador` — função, data admissão, agência lotação
- `dim_calendario` — dia útil, feriado, horário comercial

### 5.3 Tabela gold: `alertas_auditoria`

| Campo | Tipo | Descrição |
|---|---|---|
| alerta_id | string | UUID do alerta |
| transacao_id | string | FK para silver |
| dt_alerta | timestamp | Quando o alerta foi gerado |
| regra_id | string | Identificador da regra/modelo que disparou |
| familia | string | FRAUDE, PLD, CONTROLE_INTERNO |
| severidade | string | BAIXA, MEDIA, ALTA, CRITICA |
| score | float | 0.0 a 1.0 |
| motivo | string | Descrição legível para o auditor |
| status | string | ABERTO, EM_ANALISE, FALSO_POSITIVO, CONFIRMADO |

---

## 6. Detectores implementados

### 6.1 Camada estatística (auditável)

**D1 — Carta de controle CUSUM por agência**
Detecta desvios cumulativos no volume diário de transações por agência em relação à média histórica. Útil para identificar agências com aumento súbito não justificado.
- Referência: Page (1954), Montgomery (2009)

**D2 — Carta EWMA para valor médio de PIX por associado**
Média móvel exponencialmente ponderada do ticket médio de PIX. Detecta mudança gradual de comportamento (escalada).
- Parâmetros: λ=0.2, L=3

**D3 — Limites 3-sigma para concentração horária**
Distribuição esperada de transações por hora. Disparos fora de 3 desvios padrão (ex: 2h da manhã com volume de horário comercial) viram alerta.

**D4 — Lei de Benford no primeiro dígito de valores**
Em populações de transações genuínas, a distribuição do primeiro dígito segue Benford. Desvio significativo (qui-quadrado, MAD) sugere manipulação. Útil em segmentos como reembolsos, ajustes manuais, estornos.

**D5 — Detecção de fracionamento (PLD)**
Múltiplas transações de mesmo associado abaixo de limite regulatório (R$ 10.000 para PIX, por exemplo) em janela de 24h cuja soma ultrapassa o limite. Regra determinística clássica de PLD.

### 6.2 Camada de Machine Learning

**M1 — Isolation Forest multivariado por canal**
Features: valor, hora do dia, dia da semana, distância geográfica do padrão do associado, frequência nos últimos 7/30 dias, idade da conta, número de contas-destino únicas, ratio valor/saldo médio. Treinamento não-supervisionado sobre 90 dias históricos. Score de anomalia normalizado para 0–1.
- Referência: Liu, Ting, Zhou (2008)

**M2 — Autoencoder para reconstrução de perfil transacional**
Embedding de comportamento mensal do associado (vetor agregado). Autoencoder denso aprende a reconstruir o padrão "normal". Erro de reconstrução alto = comportamento atípico no mês.
- Arquitetura: 32 → 16 → 8 → 16 → 32, ReLU, MSE
- Limiar de alerta: percentil 99 do erro no conjunto de validação

**M3 — Comparação humana: análise discriminante**
Modelo supervisionado leve (Logistic Regression com regularização L2) treinado sobre os alertas históricos classificados como `CONFIRMADO` vs `FALSO_POSITIVO`. Serve para **calibrar** os detectores anteriores ao longo do tempo (feedback loop). Demonstra entendimento do ciclo de melhoria contínua em CA/CM.

### 6.3 Combinação de scores

Score final = média ponderada dos detectores ativos com pesos calibrados via análise ROC sobre amostra rotulada. Severidade derivada por bins do score:
- 0.0–0.5 → ignorado
- 0.5–0.7 → BAIXA
- 0.7–0.85 → MEDIA
- 0.85–0.95 → ALTA
- 0.95–1.0 → CRITICA

---

## 7. Dashboard Power BI

Arquivo `.pbix` versionado no Git (com `.gitattributes` apontando para Git LFS dado o tamanho).

**Página 1 — Visão executiva (auditor sênior / chefia)**
- KPIs do mês: total de alertas, % falso positivo, severidade média, tempo médio de tratamento
- Mapa de Goiás coroplético com alertas por município
- Tendência mensal por família de risco
- Top 10 agências com mais alertas (normalizado por volume)

**Página 2 — Cockpit operacional (analista de auditoria)**
- Lista filtrável de alertas abertos com severidade, score, motivo
- Filtros: agência, família de risco, período, canal, severidade
- Drill-through para detalhe da transação

**Página 3 — Análise estatística**
- Cartas CUSUM/EWMA por agência selecionada
- Distribuição Benford com chi-quadrado calculado
- Histograma de scores ML com limiar

**Página 4 — Performance dos modelos**
- Matriz de confusão dos alertas rotulados
- Taxa de falso positivo por detector
- PSI dos modelos ao longo do tempo (drift)

---

## 8. Estrutura do repositório

```
cacm/
├── README.md
├── CHANGELOG.md
├── pyproject.toml
├── poetry.lock
├── .gitignore
├── .gitattributes
├── .pre-commit-config.yaml
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── docs.yml
├── docs/
│   ├── index.md
│   ├── architecture.md
│   ├── detectors.md
│   ├── data-model.md
│   └── operations.md
├── src/
│   └── cacm/
│       ├── __init__.py
│       ├── config.py
│       ├── generators/
│       │   ├── transacoes.py
│       │   └── fraudes_sinteticas.py
│       ├── ingestion/
│       │   ├── bronze.py
│       │   └── streaming.py
│       ├── transformations/
│       │   ├── silver.py
│       │   └── gold.py
│       ├── detectors/
│       │   ├── statistical/
│       │   │   ├── cusum.py
│       │   │   ├── ewma.py
│       │   │   ├── three_sigma.py
│       │   │   ├── benford.py
│       │   │   └── structuring.py
│       │   └── ml/
│       │       ├── isolation_forest.py
│       │       ├── autoencoder.py
│       │       └── logistic_calibrator.py
│       ├── alerts/
│       │   ├── scoring.py
│       │   └── persistence.py
│       └── utils/
│           ├── spark_session.py
│           └── logging.py
├── tests/
│   ├── unit/
│   ├── integration/
│   └── fixtures/
├── notebooks/
│   ├── 01_eda.ipynb
│   ├── 02_treino_modelos.ipynb
│   └── 03_avaliacao.ipynb
├── powerbi/
│   ├── cacm.pbix
│   └── data_model.md
├── data/
│   ├── bronze/ (gitignored, gerado)
│   ├── silver/ (gitignored)
│   └── gold/   (gitignored)
└── scripts/
    ├── gerar_dados.py
    ├── rodar_pipeline.py
    └── treinar_modelos.py
```

---

## 9. Plano de execução (sprints de portfólio)

**Sprint 0 — Setup (meio dia)**
- Repositório, Poetry, pre-commit, CI mínimo, README esqueleto
- Decidir: Spark local ou Databricks Community Edition

**Sprint 1 — Geração de dados sintéticos (1 dia)**
- Gerador de transações realistas (distribuição de valor lognormal, padrões horários, sazonalidade semanal)
- Injetor de fraudes sintéticas rotuladas (ground truth para avaliar)
- Dimensões (~50 agências fictícias em Goiás, ~10.000 associados, ~200 operadores)

**Sprint 2 — Pipeline bronze/silver (1 dia)**
- Ingestão streaming simulada
- Limpeza e tipagem em silver
- Testes unitários das transformações

**Sprint 3 — Detectores estatísticos (1-2 dias)**
- CUSUM, EWMA, 3-sigma, Benford, fracionamento
- Cada detector com teste unitário e documentação

**Sprint 4 — Detectores ML (2 dias)**
- Isolation Forest com feature engineering
- Autoencoder treinado em PyTorch
- MLflow para tracking de experimentos

**Sprint 5 — Scoring combinado e tabela de alertas (meio dia)**
- Lógica de combinação ponderada
- Persistência em gold

**Sprint 6 — Power BI (1 dia)**
- Modelo dimensional consumindo gold
- Quatro páginas conforme seção 7

**Sprint 7 — Documentação e polish (meio dia)**
- README profissional com diagramas
- Vídeo curto de demo (Loom) linkado no README
- Post-mortem técnico em `docs/lessons-learned.md`

**Total estimado: 7-9 dias úteis de trabalho focado.**

---

## 10. O que destacar na entrevista (cheat sheet mental)

- **Termo certo:** "Continuous Auditing / Continuous Monitoring" (CA/CM), não "detecção de fraude genérica"
- **Justificativa arquitetural:** medallion (bronze/silver/gold) e por que essa separação importa para auditoria (rastreabilidade total)
- **Decisão consciente:** combinar estatística clássica + ML, com justificativa de cada
- **Métrica de sucesso:** precisão@k, não acurácia (auditor tem capacidade limitada de investigar — o que importa é a qualidade dos top-K alertas)
- **Feedback loop:** modelo de calibração que aprende com classificação `CONFIRMADO`/`FALSO_POSITIVO` — mostra que você pensa no ciclo de vida, não só no MVP
- **Compliance:** menção a normativos (BACEN, resolução de PLD, Circular 3.978) sem fingir expertise jurídica

---

## 11. Referências bibliográficas

- Page, E. S. (1954). *Continuous Inspection Schemes*. Biometrika.
- Montgomery, D. C. (2009). *Introduction to Statistical Quality Control*. Wiley.
- Liu, F. T., Ting, K. M., Zhou, Z. H. (2008). *Isolation Forest*. ICDM.
- IIA — Institute of Internal Auditors. *Global Technology Audit Guide (GTAG): Continuous Auditing*.
- Coderre, D. (2009). *Computer-Aided Fraud Prevention and Detection: A Step-by-Step Guide*. Wiley.
- BACEN — Circular 3.978/2020 (PLD/FTP).

---

## 12. Como rodar (versão final do README)

```bash
# 1. Clonar e instalar
git clone https://github.com/leonardozadorosny/cacm.git
cd cacm
poetry install
poetry run pre-commit install

# 2. Gerar dados sintéticos
poetry run python scripts/gerar_dados.py --dias 90 --agencias 50

# 3. Rodar pipeline completo
poetry run python scripts/rodar_pipeline.py

# 4. Treinar modelos
poetry run python scripts/treinar_modelos.py

# 5. Abrir Power BI
# powerbi/cacm.pbix
```

---

## 13. Prompt inicial para o Claude Code

Quando abrir esse projeto no Claude Code, comece com um prompt na linha:

> "Vamos construir o projeto descrito em `01-continuous-auditing-monitoring.md`. Começa pelo Sprint 0: cria a estrutura de pastas conforme seção 8, configura `pyproject.toml` com Poetry, adiciona pre-commit com ruff e mypy, e um workflow básico de CI no GitHub Actions. Não escreve código de domínio ainda — só o esqueleto e tooling. Depois passa pro Sprint 1."

Daí em diante, um sprint por iteração.
