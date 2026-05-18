# Delta Lake: Transações ACID no Data Lake

O **Delta Lake** é a camada de armazenamento que traz a confiabilidade dos bancos de dados relacionais tradicionais para os Data Lakes baseados em arquivos (como o formato Parquet armazenado em nosso bucket MinIO). Ele garante que todas as operações em nosso pipeline sigam os princípios ACID (Atomicidade, Consistência, Isolamento e Durabilidade).

---

## Viagem no Tempo (Time Travel)

No contexto de dados governamentais e despesas públicas, a auditabilidade é um requisito crítico. O Delta Lake mantém um histórico estruturado das alterações, permitindo consultar o estado exato de uma tabela em qualquer ponto do passado.

!!! tip "Caso de Uso: Auditoria de Empenhos"
    Se a situação de um `empenho` for alterada indevidamente ou registros forem excluídos acidentalmente por um script DML, podemos utilizar a funcionalidade `versionAsOf` para ler os dados exatamente como estavam antes do erro, facilitando a recuperação e a análise forense.

**Exemplo de consulta à versão inicial (0) da tabela:**
```python
# Acessando o snapshot exato da versão 0 da tabela de empenhos
spark.read.format('delta').option('versionAsOf', 0) \
    .load(bronze('empenho')) \
    .groupBy('situacao') \
    .count() \
    .orderBy('situacao') \
    .show()
```

---

## Log de Transações (Audit Log)

Toda modificação realizada nos dados (seja um Insert, Update, Delete ou Merge) é registrada de forma sequencial e detalhada no diretório de metadados `_delta_log/`. Este log atua como a fonte única da verdade, garantindo rastreabilidade total das mutações sofridas pelas tabelas ao longo de sua existência.

**Exemplo de extração do histórico de operações:**
```python
from delta.tables import DeltaTable

# Inspecionando as métricas, timestamps e tipos de operação
DeltaTable.forPath(spark, bronze('empenho')) \
    .history() \
    .select('version', 'timestamp', 'operation', 'operationMetrics') \
    .show(truncate=60)
```

---

## Evolução e Proteção de Esquema

Ao processar dados extraídos diretamente de bancos transacionais (SQL Server), é comum nos depararmos com alterações imprevistas na estrutura de origem, como a mudança de um tipo de dado ou a injeção de colunas não mapeadas.

!!! success "Schema Enforcement (Garantia de Integridade)"
    O **Schema Enforcement** é um recurso de segurança nativo do Delta Lake que atua como um "porteiro" para o Data Lake. Ele rejeita automaticamente e falha *jobs* do Spark que tentem inserir dados com esquemas incompatíveis com a estrutura registrada da tabela. Isso impede a corrupção física dos arquivos, exigindo que qualquer alteração de esquema (Schema Evolution) seja declarada explicitamente no código pelo Engenheiro de Dados.
