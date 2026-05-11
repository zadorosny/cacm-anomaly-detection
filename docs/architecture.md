# Arquitetura

Pipeline medallion (bronze → silver → gold) com Apache Spark, alimentando um cockpit Power BI.

```
Faker generator ──► JSON / Parquet
                     │
                     ▼
              ┌──────────────┐
              │  BRONZE      │ ← Structured Streaming (opcional)
              │  Parquet     │   particionado por dt × agência
              └──────┬───────┘
                     ▼
              ┌──────────────┐
              │  SILVER      │ ← limpeza, dedup, enriquecimento com dimensões,
              │  Parquet     │   flags determinísticas (fora_horario, valor_alto…)
              └──────┬───────┘
                     ▼
        ┌────────────┴────────────┐
        ▼                         ▼
┌──────────────┐         ┌──────────────────────┐
│ GOLD facts   │         │ Detectores           │
│ (agg.)       │         │ stat: CUSUM, EWMA,   │
│              │         │       3σ, Benford,   │
│              │         │       fracionamento  │
│              │         │ ml:   IF, Autoencoder│
└──────┬───────┘         └──────────┬───────────┘
       │                            │
       │                            ▼
       │                ┌──────────────────────┐
       │                │ alertas_auditoria    │
       │                │ (gold)               │
       │                └──────────┬───────────┘
       ▼                           ▼
            Power BI cockpit (4 páginas)
```

## Decisões arquiteturais

### Por que medallion?
- **Rastreabilidade total para auditoria.** Cada camada é imutável e responde a perguntas diferentes: *"o que entrou?"* (bronze), *"como foi tratado?"* (silver), *"o que vamos investigar?"* (gold).
- **Reprocessabilidade.** Posso recriar silver a partir do bronze sem voltar à fonte original. Crítico em ambiente regulado.

### Por que combinar estatística clássica + ML?
- **Estatística clássica é defensável em conselho.** CUSUM/Benford são técnicas centenárias com literatura consolidada e comportamento previsível.
- **ML pega o que regras não pegam.** Isolation Forest detecta padrões multivariados que ninguém escreveria como regra explícita.
- A combinação eleva precisão@k (o que importa em auditoria, dado que o auditor tem capacidade limitada de investigação).

### Por que Parquet (não CSV/JSON)?
- Compressão columnar reduz I/O e custo de armazenamento.
- Schema embutido evita ambiguidades de tipo em re-leitura.
- Particionamento por `dt × agencia_id` acelera filtros típicos do cockpit.

### Trade-offs assumidos
- **PySpark local em vez de Databricks.** Mais simples para portfólio, ao custo de não exibir UI Spark/jobs distribuídos reais.
- **Pandas no scoring.** Como o volume de portfólio cabe em memória, manipulamos com pandas após `toPandas()`. Em produção, detectores ML rodariam via Spark MLlib ou pandas UDF.
- **MLflow local (file store).** Em produção, MLflow Tracking Server + S3 backend.
