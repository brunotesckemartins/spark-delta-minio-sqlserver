# Tabelas Gerenciadas vs. Não Gerenciadas

Ao trabalhar com o Spark Catalog e salvar DataFrames utilizando a instrução `CREATE TABLE`, estabelecemos o nível de gerenciamento físico e lógico que o Spark terá sobre os dados.

## Tabelas Não Gerenciadas (Unmanaged Tables)

Abordagem utilizada ativamente neste projeto.

* **Conceito:** O desenvolvedor define especificamente o diretório onde os dados serão salvos através da cláusula `LOCATION`.
* **Comportamento:** O Spark administra **apenas os metadados** no catálogo. 
* **Segurança:** A execução de um comando `DROP TABLE` remove exclusivamente os registros metadados do Hive Metastore/Catalog, porém **não** apaga os dados físicos contidos no armazenamento em nuvem (MinIO).

## Tabelas Gerenciadas (Managed Tables)

Abordagem padrão em instâncias simples de armazenamento interno.

* **Conceito:** O Spark gerencia completamente tanto o registro no catálogo quanto o armazenamento físico dos dados na estrutura de diretórios do *warehouse* interno.
* **Comportamento:** A vida útil do dado está atrelada à vida útil da tabela.
* **Risco:** Executar `DROP TABLE` elimina simultaneamente os metadados e os arquivos físicos no sistema de origem, de forma irreversível.