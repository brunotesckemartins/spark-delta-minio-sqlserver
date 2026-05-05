# 03 - Manipulação de Dados (DML) no Delta Lake

Demonstramos a capacidade transacional do ambiente gravado no bucket `bronze`, operando diretamente sobre os arquivos através de rotinas DML (Data Manipulation Language).

## Inserção com MERGE (Idempotência)

Utilizamos a API `DeltaTable` para processar um `merge`, assegurando que registros não sejam duplicados na tabela de fornecedores/credores.

```python
dt_credor = DeltaTable.forPath(spark, bronze('credor'))

(
    dt_credor.alias('dst')
    .merge(novos.alias('src'), 'dst.id_credor = src.id_credor')
    .whenNotMatchedInsertAll()
    .execute()
)
```

## Atualização de Dados (UPDATE)

Atualizamos a coluna `municipio` dos credores de São Paulo diretamente no Data Lake.

```python
dt_credor = DeltaTable.forPath(spark, bronze('credor'))
dt_credor.update(
    condition = F.col('uf') == 'SP',
    set = {'municipio': F.concat(F.col('municipio'), F.lit(' - Capital'))}
)
```

## Exclusão de Registros (DELETE)

Exclusão transacional que remove pagamentos marcados como 'Cancelado'. O histórico dessa ação é preservado nos logs do Delta.

```python
dt_pagamento = DeltaTable.forPath(spark, bronze('pagamento'))
dt_pagamento.delete(condition = F.col('situacao') == 'Cancelado')
```