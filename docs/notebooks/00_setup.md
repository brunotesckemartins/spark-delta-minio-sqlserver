# 00 - Setup do SQL Server

Nesta etapa, construímos a fundação do projeto. Este notebook simula o sistema transacional origem. Criamos o banco `DespesaPublicaDB`, geramos 11 tabelas estruturadas e realizamos a carga inicial via PySpark JDBC.

## Inicialização do Spark com JDBC

O PySpark é configurado para baixar o driver `mssql-jdbc` automaticamente via Maven.

```python
spark = (
    SparkSession.builder
    .appName('DespesaPublica-Setup')
    .config(
        'spark.jars.packages',
        'com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11'
    )
    .getOrCreate()
)
```

## Criação e Carga de Dados (CSV para SQL Server)

Após a criação das tabelas via `sqlcmd`, iteramos sobre os arquivos locais e os inserimos no SQL Server usando o modo `append`.

```python
for tabela in TABELAS:
    sqlcmd(f'DELETE FROM {tabela}', db=DATABASE)

    df = (
        spark.read
        .option('header', 'true')
        .option('inferSchema', 'true')
        .csv(f'{DATA_DIR}/{tabela}.csv')
    )

    df.write.jdbc(
        url=JDBC_DB,
        table=tabela,
        mode='append',
        properties=JDBC_PROPS
    )
```