# Delta Lake

O **Delta Lake** fornece garantias ACID (Atomicidade, Consistência, Isolamento e Durabilidade) escaláveis sobre Data Lakes. Ele atua como uma camada de armazenamento que traz confiabilidade a lagos baseados em Parquet.

## Time Travel (Viagem no Tempo)

O Delta guarda um histórico das alterações utilizando versionamento. Isso possibilita consultar ou reverter dados como eles estavam em um determinado momento, usando a funcionalidade `versionAsOf`.

```python
spark.read.format('delta').option('versionAsOf', 0) \
    .load(bronze('empenho')) \
    .groupBy('situacao').count().orderBy('situacao').show()
```

## Audit Log

Cada operação (Write, Update, Delete) fica detalhadamente registrada no diretório oculto `_delta_log/`. Essa trilha permite total rastreabilidade das mutações sofridas pelas tabelas ao longo de sua existência.

```python
DeltaTable.forPath(spark, bronze('empenho')) \
    .history() \
    .select('version', 'timestamp', 'operation', 'operationMetrics') \
    .show(truncate=60)
```

!!! success "Schema Enforcement"
    Recurso de segurança que rejeita silenciosamente *jobs* que tentem inserir dados com esquemas incompatíveis com a estrutura original da tabela, impedindo a corrupção física do Data Lake.