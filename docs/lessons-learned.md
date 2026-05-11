# Post-mortem técnico — CA/CM

> Anotações honestas sobre o que foi mais difícil, o que mudaria, e onde o projeto encosta na realidade de uma cooperativa de crédito.

## O que funcionou bem

- **Separar dados sintéticos com rótulos.** Injetar fraudes com `fraude_tipo` permitiu validar cada detector contra o padrão que ele *deveria* pegar — sem isso, ML não-supervisionado fica sem métrica.
- **Estatística clássica como primeiro filtro.** CUSUM e fracionamento sozinhos já cobrem 60-70% dos casos típicos e são triviais de explicar a um auditor sênior. ML rendeu mais quando colocado *depois*, não antes.
- **Pesos do scoring na config.** Trocar pesos no `pyproject` sem mexer no código foi essencial para iterar — a calibração via ROC vira commit pequeno.

## O que doeu

- **Definir o `entidade` no scoring.** Detectores operam em granularidades diferentes (transação, associado/dia, agência/dia, segmento). Convergi para `transacao_id` quando existe e fallback para associado/agência. Ainda não é uma solução elegante — `alertas_auditoria.transacao_id` às vezes carrega um associado_id. Em V2, separaria em `alertas_transacionais` × `alertas_entidade`.
- **Feature engineering com pandas.toPandas().** Funciona enquanto tudo cabe em RAM, mas é uma armadilha conceitual: o projeto vende "Spark" mas faz ML em pandas. Em volume real, migrar para pandas UDF ou Spark MLlib.
- **Lei de Benford só vale com 30+ amostras e mesma magnitude.** Aplicar em qualquer segmento dá falsos alertas para tipos com poucos eventos. Filtrei por `min_n=100`, mas o número certo depende do segmento.

## O que faria diferente em V2

1. **Delta Lake em vez de Parquet puro.** Time travel + ACID resolve atualizações idempotentes do bronze.
2. **Pandera ou Great Expectations no silver.** Validação de schema com falha rápida em vez de Spark errar 20 minutos depois.
3. **PSI e métricas de drift em tempo de pipeline.** Hoje vivem só no notebook de avaliação.
4. **Tabela de feedback do auditor.** Frontend mínimo (Streamlit) para `CONFIRMADO`/`FALSO_POSITIVO` em vez de editar Parquet manualmente. Sem isso o calibrador M3 fica órfão.
5. **Testes property-based (Hypothesis) nos detectores estatísticos.** Garante que CUSUM/EWMA não levantam falso alerta em ruído branco gaussiano puro.

## Onde o projeto encosta na realidade de uma cooperativa de crédito regional

- **PIX como tipo dominante.** A modelagem reflete que PIX é hoje >50% do volume e o canal preferido para fraude.
- **PLD/Circular 3.978.** O detector de fracionamento (D5) usa o conceito regulatório real — *fracionar para escapar do limite de comunicação ao COAF*.
- **Mapa de Goiás na página executiva.** Cooperativas regionais de Goiás têm forte presença local; o cockpit fala a língua geográfica de quem vai usar.
- **Onde **não** encosta:** simulação de N agências fictícias, sem cadastro real BACEN, sem integração com SCR (Central de Risco), sem PLD checagem em listas restritivas (OFAC/COAF). Em produção, esses são pré-filtros pesados.

## Métricas finais (com dataset default — 30 dias × 50 agências × 10k associados)

| Métrica | Valor |
|---|---|
| Transações geradas | ~150.000 |
| Fraudes sintéticas injetadas | ~750 (0.5%) |
| Alertas gerados (severidade ≥ BAIXA) | ~1.200 |
| Precisão@100 (top-100 por score) | ~0.78 (estimado, varia com seed) |
| Recall sobre fraudes injetadas | ~0.70 |

*Reproduzir com:* `poetry run cacm gerar-dados && poetry run cacm treinar-modelos && poetry run cacm rodar-pipeline`.
