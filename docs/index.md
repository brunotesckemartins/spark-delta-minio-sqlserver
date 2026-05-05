# Spark + Delta Lake + MinIO - Despesa Pública

Este projeto consiste na construção de um pipeline de engenharia de dados que extrai informações financeiras governamentais de um banco relacional, transporta-as para um Data Lake e as estrutura de forma otimizada para análise, utilizando um ecossistema moderno de Big Data.

## Decisões Arquiteturais e Tecnológicas

A arquitetura do projeto foi desenhada visando modularidade, portabilidade e segurança dos dados.

* **Comunicação sem ODBC (100% JDBC e Docker):** Optou-se por eliminar a dependência de instalações complexas de drivers ODBC no sistema operacional. A criação do banco e das tabelas (DDL) ocorre isoladamente via utilitário `sqlcmd` dentro do próprio container Docker do SQL Server. Toda a transferência de dados (DML) foi configurada para utilizar o protocolo JDBC através do PySpark.
* **Gerenciamento Dinâmico de Dependências (Maven):** O driver do SQL Server (`mssql-jdbc`) e os pacotes necessários para comunicar com o S3/MinIO (`hadoop-aws` e `aws-java-sdk-bundle`) não requerem instalação prévia no SO. Eles são injetados na inicialização da `SparkSession` e baixados dinamicamente via Maven.
* **Tabelas Não Gerenciadas (Unmanaged Tables):** Na camada Bronze, as tabelas foram criadas com o parâmetro `LOCATION` apontando para o MinIO. Isso significa que as tabelas são "não gerenciadas", onde o Spark administra apenas os metadados no catálogo, enquanto os dados físicos pertencem ao MinIO.

!!! info "Idempotência"
    A atualização das bases utiliza comandos robustos como o `MERGE` para inserções, impedindo a duplicação de dados caso o script seja rodado múltiplas vezes.