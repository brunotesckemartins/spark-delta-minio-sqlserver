# 01 - Ingestão para Landing Zone (MinIO)

A fase de ingestão bruta. Retiramos os dados do banco relacional e os movemos para o armazenamento em nuvem no bucket `landing-zone`.

## Configuração do Spark

Nesta etapa, o PySpark necessita dos drivers do SQL Server e das bibliotecas do AWS S3 (`hadoop-aws`) para a comunicação unificada.

```python
spark = (
    SparkSession.builder
    .appName('DespesaPublica-01-SQLServer-to-MinIO')
    .config(
        'spark.jars.packages',
        'com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11,'
        'org.apache.hadoop:hadoop-aws:3.3.4,'
        'com.amazonaws:aws-java-sdk-bundle:1.12.367'
    )
    .config('spark.hadoop.fs.s3a.endpoint',          MINIO_ENDPOINT)
    .config('spark.hadoop.fs.s3a.access.key',        MINIO_ACCESS_KEY)
    .config('spark.hadoop.fs.s3a.secret.key',        MINIO_SECRET_KEY)
    .config('spark.hadoop.fs.s3a.path.style.access', 'true')
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .getOrCreate()
)
```

## Extração via PySpark

Usamos a função `coalesce(1)` para forçar o Spark a gravar cada tabela em um único arquivo CSV, facilitando a governança na zona de pouso.

```python
for tabela in TABELAS:
    df = spark.read.jdbc(url=JDBC_URL, table=tabela, properties=JDBC_PROPS)

    csv_path = f's3a://{BUCKET_LANDING}/{tabela}'
    (
        df.coalesce(1)
        .write
        .mode('overwrite')
        .option('header', 'true')
        .csv(csv_path)
    )
```