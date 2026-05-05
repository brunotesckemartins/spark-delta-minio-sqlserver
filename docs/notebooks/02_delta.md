# 02 - Refinamento para Delta Lake (Bronze)

O refinamento converte os dados legados para um formato colunar de alto desempenho. Lemos os arquivos CSV não-tipados da `landing-zone`, aplicamos o schema rigoroso e gravamos no bucket `bronze` no formato Delta.

## Spark Session Integrada ao Delta

A lista de dependências do S3 é passada através da função `extra_packages` do gerenciador nativo do Delta Lake para evitar sobrescritas de configuração.

```python
EXTRA_PACKAGES = [
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.367"
]

builder = (
    SparkSession.builder
    .appName('DespesaPublica-02-CSV-to-Delta')
    .config('spark.sql.extensions',
            'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog',
            'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    .config('spark.hadoop.fs.s3a.endpoint',          MINIO_ENDPOINT)
    .config('spark.hadoop.fs.s3a.access.key',        MINIO_ACCESS_KEY)
    .config('spark.hadoop.fs.s3a.secret.key',        MINIO_SECRET_KEY)
    .config('spark.hadoop.fs.s3a.path.style.access', 'true')
    .config('spark.hadoop.fs.s3a.impl',
            'org.apache.hadoop.fs.s3a.S3AFileSystem')
    .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
)

spark = configure_spark_with_delta_pip(builder, extra_packages=EXTRA_PACKAGES).getOrCreate()
```

## Criação e Registro das Delta Tables

Adicionamos colunas de auditoria aos DataFrames (`dt_carga` e `origem`), salvamos em formato `delta` e as registramos no catálogo Spark apontando para a localização remota no MinIO.

```python
    df = (
        df
        .withColumn('dt_carga', F.current_timestamp())
        .withColumn('origem',   F.lit('sqlserver -> landing-zone -> bronze'))
    )

    delta_path = f's3a://{BUCKET_BRONZE}/{tabela}'
    df.write.format('delta').mode('overwrite').save(delta_path)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS despesa_publica.{tabela}
        USING DELTA
        LOCATION '{delta_path}'
    """)
```