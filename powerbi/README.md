# Power BI — Cockpit de Auditoria

> O arquivo `cacm.pbix` será gerado a partir das instruções abaixo. **Versionado via Git LFS.**

## Setup inicial (uma vez)

```bash
git lfs install
git lfs track "*.pbix"
git add .gitattributes
```

## Montagem do cockpit

1. Garantir que `poetry run cacm rodar-pipeline` foi executado — `data/gold/` deve conter os Parquets.
2. Abrir Power BI Desktop → `Get Data` → `Folder` → apontar para `data/gold/`.
3. Para cada subpasta (`alertas_auditoria`, `fato_agencia_dia`, etc.), usar a query M em [`queries/power_query_alertas.m`](./queries/power_query_alertas.m) como template.
4. Aplicar relacionamentos descritos em [`data_model.md`](./data_model.md).
5. Importar medidas DAX da seção 3 de `data_model.md`.
6. Construir as 4 páginas conforme especificação.
7. Salvar como `powerbi/cacm.pbix`.

## Referência

Ver [`data_model.md`](./data_model.md) para esquema, relacionamentos, medidas e páginas.
