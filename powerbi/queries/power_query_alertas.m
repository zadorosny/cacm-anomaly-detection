// Power Query (M) — leitura do parquet de alertas
let
    Source = Folder.Files("..\data\gold\alertas_auditoria"),
    ParquetFiles = Table.SelectRows(Source, each Text.EndsWith([Name], ".parquet")),
    ExpandContent = Table.AddColumn(ParquetFiles, "Data", each Parquet.Document([Content])),
    UnionRows = Table.Combine(ExpandContent[Data]),
    TipagemFinal = Table.TransformColumnTypes(UnionRows, {
        {"alerta_id", type text},
        {"transacao_id", type text},
        {"dt_alerta", type datetime},
        {"regra_id", type text},
        {"familia", type text},
        {"severidade", type text},
        {"score", type number},
        {"motivo", type text},
        {"status", type text}
    })
in
    TipagemFinal
