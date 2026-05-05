# Setup do Ambiente

O ecossistema deste projeto é baseado em containers Docker para os serviços de armazenamento e banco de dados, e um ambiente virtual Python isolado para o processamento de dados com Apache Spark.

## Serviços Locais

Os seguintes serviços rodam de forma independente em containers:

* **SQL Server:** Atua como o banco de dados transacional de origem. Acessível via `localhost:1433`.
* **MinIO:** Atua como o nosso Object Storage (Data Lake) simulando o Amazon S3. Acessível via `http://localhost:9020`.

## Variáveis de Ambiente

O projeto utiliza um arquivo `.env` na raiz para injetar as credenciais de forma segura. O PySpark consome essas variáveis para autenticar tanto no SQL Server quanto no MinIO.

```python
import os
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT   = os.getenv('MINIO_ENDPOINT',   'http://localhost:9020')
MINIO_ACCESS_KEY = os.getenv('MINIO_ACCESS_KEY', 'minioadmin')
MINIO_SECRET_KEY = os.getenv('MINIO_SECRET_KEY', 'minioadmin')
```