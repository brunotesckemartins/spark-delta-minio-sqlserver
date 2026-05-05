# Projeto Apache Spark + Delta Lake + MinIO + SQL Server
### Dataset: Despesa Pública Federal — DespesaPublicaDB

Projeto desenvolvido para o curso de **Engenharia de Dados com Spark e Delta Lake**.  
Implementa um pipeline completo de dados com o tema de **Administração - Despesa Pública Federal**, inspirado nos dados abertos do [Portal de Dados Abertos do Governo Federal](https://dados.gov.br/dados/conjuntos-dados/administracao---despesa-publica---empenhos).

---

## Arquitetura do Pipeline

```text
┌─────────────────────┐       ┌──────────────────────┐       ┌─────────────────────────┐
│   SQL Server 2022   │──────▶│   MinIO (S3)         │──────▶│   MinIO (S3)            │
│   DespesaPublicaDB  │ JDBC  │   bucket:            │ Spark │   bucket:               │
│                     │       │   landing-zone/      │ Delta │   bronze/               │
│   11 tabelas        │       │                      │       │                         │
│   relacionais       │       │   1 CSV por tabela   │       │   Delta Tables          │
└─────────────────────┘       └──────────────────────┘       │   INSERT/UPDATE/DELETE  │
      Notebook 00                    Notebook 01             │   HISTORY/TIME TRAVEL   │
      (Setup e carga)                (Extração)              └─────────────────────────┘
                                                               Notebooks 02 e 03
```

> **Sem ODBC.** Toda comunicação com o SQL Server é feita via **JDBC** pelo PySpark. O driver JDBC é baixado automaticamente pelo Spark via Maven Central. Nenhuma instalação de driver nativo no sistema operacional é necessária.

---

## Dataset: DespesaPublicaDB

Banco relacional com **11 tabelas** modelando o ciclo completo de execução orçamentária federal:

| Tabela | Descrição | Registros |
|--------|-----------|:---------:|
| `orgao` | Órgãos federais (MEC, MS, MJSP, MCTI, MMA) | 5 |
| `unidade` | Unidades gestoras vinculadas aos órgãos | 11 |
| `programa` | Programas de governo | 8 |
| `acao` | Ações orçamentárias | 12 |
| `fonte_recurso` | Fontes de financiamento | 6 |
| `credor` | Fornecedores/credores do governo | 15 |
| `empenho` | Comprometimento de despesa | 50 |
| `item_empenho` | Itens detalhados de cada empenho | 126 |
| `liquidacao` | Liquidações de despesa | 40 |
| `pagamento` | Pagamentos realizados | 35 |
| `historico_preco` | Histórico de preços de referência | 15 |

---

## Pré-requisitos

| Ferramenta | Versão mínima |
|------------|--------------|
| Docker + Docker Compose v2 | qualquer |
| Python | 3.11 |
| Java JDK | 11 |
| UV (gerenciador de pacotes) | qualquer |
| Git | qualquer |

---

## Instalação por Sistema Operacional

### Ubuntu / Debian (Recomendado para WSL)

**1. Atualizar e instalar dependências base**

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y git curl
```

**2. Java 11**

```bash
sudo apt install -y openjdk-11-jdk

echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

**3. Python 3.11 via Pyenv**

```bash
sudo apt install -y make build-essential libssl-dev zlib1g-dev \
libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev

curl [https://pyenv.run](https://pyenv.run) | bash

echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
echo 'command -v pyenv >/dev/null || export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
echo 'eval "$(pyenv init -)"' >> ~/.bashrc
source ~/.bashrc

pyenv install 3.11.9
pyenv global 3.11.9
```

**4. UV**

```bash
curl -LsSf [https://astral.sh/uv/install.sh](https://astral.sh/uv/install.sh) | sh
source ~/.bashrc
```

**5. Docker**

```bash
sudo apt install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL [https://download.docker.com/linux/ubuntu/gpg](https://download.docker.com/linux/ubuntu/gpg) -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] [https://download.docker.com/linux/ubuntu](https://download.docker.com/linux/ubuntu) $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

sudo usermod -aG docker $USER
newgrp docker
```

---

### Arch Linux / Manjaro

**1. Java 11**

```bash
sudo pacman -S jdk11-openjdk
sudo archlinux-java set java-11-openjdk

echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

**2. Python 3.11**

```bash
sudo pacman -S pyenv
pyenv install 3.11.9
pyenv global 3.11.9
```

**3. Docker**

```bash
sudo pacman -S docker docker-compose
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
newgrp docker
```

---

## Configuração do Projeto

### 1. Clonar o repositório

```bash
git clone [https://github.com/brunotesckemartins/spark-delta-minio-despesa.git](https://github.com/brunotesckemartins/spark-delta-minio-despesa.git)
cd spark-delta-minio-despesa
```
### 1.1
Copie o .env.example e cole o conteúdo dele em .env(crie na raiz do projeto)

### 2. Subir os containers

```bash
docker compose up -d
```

| Container | Imagem | Portas |
|-----------|--------|--------|
| `sqlserver-despesa` | `mcr.microsoft.com/mssql/server:2022-latest` | `1433` |
| `minio-despesa` | `minio/minio` (Feb 2025) | `9020` (API), `9021` (Console) |

### 3. Ambiente virtual e dependências

```bash
uv venv
source .venv/bin/activate
uv sync
```

### 4. Documentação Técnica (MkDocs)

O projeto conta com uma documentação detalhada sobre as decisões arquiteturais e funcionamento dos notebooks. Para acessar:

```bash
uv pip install mkdocs mkdocs-material
mkdocs serve
```
Acesse [http://127.0.0.1:8000](http://127.0.0.1:8000).

---

## Execução dos Notebooks

Execute os arquivos na pasta `notebook/` seguindo a ordem lógica do pipeline:

| # | Notebook | Função |
|---|----------|-----------|
| 0 | `00_setup_sqlserver.ipynb` | Criação do banco, tabelas e carga JDBC inicial |
| 1 | `01_sqlserver_to_minio_csv.ipynb` | Extração do SQL Server para CSV no MinIO (Landing Zone)[cite: 4] |
| 2 | `02_csv_to_delta.ipynb` | Refinamento: CSV para Delta Lake (Bronze Zone) |
| 3 | `03_dml_delta.ipynb` | Operações transacionais, History e Time Travel no Delta |

---

## Credenciais Padrão

| Serviço | Usuário | Senha |
|---------|---------|-------|
| SQL Server | `sa` | `SqlServer@2024!` |
| MinIO | `minioadmin` | `minioadmin` |

## Login MInIO

acesse o MinIO em : 
localhost:9021

login : minioadmin
senha : minioadmin
---

## Tecnologias Utilizadas

| Tecnologia | Papel |
|------------|-------|
| Apache Spark | Processamento distribuído |
| Delta Lake | Camada de armazenamento ACID |
| MinIO | Object Storage S3-Compatible |
| SQL Server | Banco de dados transacional de origem |
| UV | Gerenciamento de ambiente e pacotes Python |
| MkDocs | Documentação técnica do projeto |

---

## Tabelas Gerenciadas vs Não Gerenciadas

Este projeto utiliza **Tabelas Não Gerenciadas**:
* **Dados**: Armazenados no MinIO (`s3a://bronze/`).
* **Metadados**: Gerenciados pelo Spark Catalog.
* **DROP TABLE**: Remove apenas o registro no catálogo; os arquivos físicos permanecem seguros no MinIO.
