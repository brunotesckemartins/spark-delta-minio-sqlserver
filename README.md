# Projeto Apache Spark + Delta Lake + MinIO + SQL Server
### Dataset: Despesa Pública Federal — DespesaPublicaDB

Projeto desenvolvido para o curso de **Engenharia de Dados com Spark e Delta Lake**.  
Implementa um pipeline completo de dados com o tema de **Administração - Despesa Pública Federal**,
inspirado nos dados abertos do [Portal de Dados Abertos do Governo Federal](https://dados.gov.br/dados/conjuntos-dados/administracao---despesa-publica---empenhos).

---

## Arquitetura do Pipeline

```
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

> **Sem ODBC.** Toda comunicação com o SQL Server é feita via **JDBC** pelo PySpark.
> O driver JDBC é baixado automaticamente pelo Spark via Maven Central.
> Nenhuma instalação de driver nativo no sistema operacional é necessária.

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

> **ODBC não é necessário.** O driver JDBC do SQL Server é gerenciado pelo Spark via Maven.

---

## Instalação por Sistema Operacional

### Ubuntu / Debian

**1. Atualizar e instalar dependências base**

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y git curl
```

**2. Java 11**

```bash
sudo apt install -y openjdk-11-jdk
java -version
```

Adicionar ao `~/.bashrc`:

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

**3. Python 3.11**

```bash
sudo apt install -y python3.11 python3.11-venv python3.11-dev
python3.11 --version
```

**4. UV**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc
uv --version
```

**5. Docker**

```bash
sudo apt install -y ca-certificates curl
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
     -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

echo "deb [arch=$(dpkg --print-architecture) \
  signed-by=/etc/apt/keyrings/docker.asc] \
  https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

sudo usermod -aG docker $USER
newgrp docker

docker --version
docker compose version
```

---

### Arch Linux / Manjaro

**1. Atualizar o sistema**

```bash
sudo pacman -Syu
```

**2. Java 11**

```bash
sudo pacman -S jdk11-openjdk
sudo archlinux-java set java-11-openjdk
java -version
```

Adicionar ao `~/.bashrc` (ou `~/.zshrc` se usar zsh):

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

**3. Python 3.11**

```bash
sudo pacman -S python python-pip
python --version
```

Arch já mantém Python atualizado. Se precisar fixar na 3.11 especificamente:

```bash
sudo pacman -S pyenv
pyenv install 3.11.9
pyenv global 3.11.9
```

**4. UV**

Via script oficial:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc
```

Ou via AUR:

```bash
yay -S uv      # com yay
# ou
paru -S uv     # com paru
```

**5. Docker**

```bash
sudo pacman -S docker docker-compose
sudo systemctl enable --now docker
sudo usermod -aG docker $USER
newgrp docker

docker --version
docker compose version
```

**6. Git**

```bash
sudo pacman -S git
```

---

### Fedora / RHEL / CentOS Stream

**1. Atualizar o sistema**

```bash
sudo dnf update -y
sudo dnf install -y git curl
```

**2. Java 11**

```bash
sudo dnf install -y java-11-openjdk java-11-openjdk-devel
java -version
```

Configurar `JAVA_HOME`. O caminho varia conforme a versão instalada — use `alternatives` para descobrir:

```bash
alternatives --list | grep java
# Exemplo de saída: /usr/lib/jvm/java-11-openjdk-11.x.x.x-amd64/bin/java
```

Adicionar ao `~/.bashrc` (ou `~/.zshrc` se usar zsh):

```bash
echo 'export JAVA_HOME=/usr/lib/jvm/java-11-openjdk' >> ~/.bashrc
echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
source ~/.bashrc
```

**3. Python 3.11**

Fedora 38+ já inclui Python 3.11 nos repositórios oficiais:

```bash
sudo dnf install -y python3.11 python3.11-devel
python3.11 --version
```

Em versões mais antigas do Fedora ou no RHEL/CentOS Stream, use o repositório `crb` + `epel`:

```bash
# Habilitar repositórios extras (RHEL/CentOS Stream 9)
sudo dnf install -y epel-release
sudo dnf config-manager --set-enabled crb
sudo dnf install -y python3.11 python3.11-devel
```

**4. UV**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc
uv --version
```

**5. Docker**

```bash
sudo dnf -y install dnf-plugins-core
sudo dnf config-manager --add-repo https://download.docker.com/linux/fedora/docker-ce.repo

sudo dnf install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin

sudo systemctl enable --now docker
sudo usermod -aG docker $USER
newgrp docker

docker --version
docker compose version
```

> No **RHEL / CentOS Stream**, substitua a URL do repositório:
> ```bash
> sudo dnf config-manager --add-repo https://download.docker.com/linux/rhel/docker-ce.repo
> ```

**6. Git**

```bash
sudo dnf install -y git
```

---

### Windows 10 / 11

Existem duas opções. A **opção WSL 2** é a recomendada pois o Spark roda com mais estabilidade no Linux.

#### Opção A — WSL 2 (recomendada)

**1. Instalar WSL 2 com Ubuntu**

Abra o PowerShell como Administrador:

```powershell
wsl --install -d Ubuntu-22.04
```

Reinicie o computador. Após reiniciar configure usuário e senha no Ubuntu.

Verificar se está na versão 2:

```powershell
wsl --list --verbose
# Ubuntu-22.04   Running  2
```

**2. Instalar Docker Desktop**

Baixe em [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/).

Nas configurações: `Settings → Resources → WSL Integration` → habilitar para Ubuntu-22.04.

**3. Seguir as instruções do Ubuntu**

Abra o terminal Ubuntu (WSL) e siga exatamente os passos da seção **Ubuntu / Debian** acima.

---

#### Opção B — Windows Nativo (PowerShell)

**1. Java 11**

Baixe o instalador `.msi` do [Adoptium Temurin 11](https://adoptium.net/temurin/releases/?version=11).

Após instalar, configure as variáveis de ambiente via PowerShell (como Administrador):

```powershell
[System.Environment]::SetEnvironmentVariable("JAVA_HOME","C:\Program Files\Eclipse Adoptium\jdk-11.x.x.x-hotspot","Machine")
$path = [System.Environment]::GetEnvironmentVariable("Path","Machine")
[System.Environment]::SetEnvironmentVariable("Path","$path;%JAVA_HOME%\bin","Machine")
```

Reabra o terminal e verifique:

```cmd
java -version
```

**2. Python 3.11**

Baixe em [python.org/downloads](https://www.python.org/downloads/release/python-3119/).
Durante a instalação marque obrigatoriamente: **"Add Python to PATH"**.

```cmd
python --version
```

**3. UV**

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Reabra o terminal:

```cmd
uv --version
```

**4. Docker Desktop**

Baixe em [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop/).
Habilite o **WSL 2 backend** em Settings → General.

```cmd
docker --version
docker compose version
```

**5. Git**

Baixe em [git-scm.com/download/win](https://git-scm.com/download/win).

---

## Configuração do Projeto

Os passos abaixo são iguais para todos os sistemas (use o terminal Linux ou WSL):

### 1. Clonar o repositório

```bash
git clone https://github.com/seu-usuario/spark-delta-minio-despesa.git
cd spark-delta-minio-despesa
```

### 2. Subir os containers

```bash
docker compose up -d
```

| Container | Imagem | Portas |
|-----------|--------|--------|
| `sqlserver-despesa` | `mcr.microsoft.com/mssql/server:2022-latest` | `1433` |
| `minio-despesa` | `minio/minio` (Feb 2025) | `9020` (API), `9021` (Console) |

Aguarde ~20 segundos para o SQL Server inicializar. Testar:

```bash
docker compose ps

docker exec sqlserver-despesa \
  /opt/mssql-tools18/bin/sqlcmd \
  -S localhost,1433 -U sa -P 'SqlServer@2024!' -C \
  -Q "SELECT @@VERSION"
```

Console MinIO: [http://localhost:9021](http://localhost:9021) — login `minioadmin` / `minioadmin`

### 3. Variáveis de ambiente

```bash
cp .env.example .env
# Os valores padrão já correspondem ao docker-compose
```

### 4. Ambiente virtual e dependências

```bash
uv venv
source .venv/bin/activate       # Linux / WSL / macOS
# .venv\Scripts\activate        # Windows nativo (PowerShell)

uv sync
```

### 5. Executar os notebooks

Você tem duas opções: **JupyterLab no navegador** ou **VS Code**. Escolha a que preferir.

---

#### Opção A — JupyterLab no navegador

**Sem senha / sem token (recomendado para uso local)**

Por padrão o JupyterLab gera um token aleatório toda vez que inicia, o que exige colar o token na tela de login. Para desativar isso em ambiente local, rode uma vez:

```bash
jupyter lab --generate-config
```

Isso cria o arquivo `~/.jupyter/jupyter_lab_config.py`. Abra-o e adicione as duas linhas abaixo no final (ou procure e descomente as variáveis já existentes):

```python
c.ServerApp.token = ''
c.ServerApp.password = ''
```

Agora inicie normalmente — o navegador abrirá sem pedir nada:

```bash
jupyter lab
```

Acesse [http://localhost:8888](http://localhost:8888).

> **Alternativa rápida** (sem editar o arquivo de config): passe os parâmetros direto no comando:
> ```bash
> jupyter lab --ServerApp.token='' --ServerApp.password=''
> ```

**Selecionar o kernel correto**

Ao abrir qualquer notebook, no canto superior direito clique em **"Select Kernel"** → escolha **`.venv (Python 3.11)`**. Se não aparecer, rode antes:

```bash
python -m ipykernel install --user --name=.venv --display-name "Python 3.11 (.venv)"
```

---

#### Opção B — VS Code (recomendada)

O VS Code executa notebooks `.ipynb` nativamente, sem precisar abrir nenhum navegador.

**1. Instalar o VS Code**

| Sistema | Download |
|---------|---------|
| Ubuntu / Debian | [code.visualstudio.com](https://code.visualstudio.com/download) — baixe o `.deb` e instale com `sudo dpkg -i code_*.deb` |
| Fedora / RHEL | Baixe o `.rpm` e instale com `sudo dnf install code_*.rpm` |
| Arch / Manjaro | `yay -S visual-studio-code-bin` |
| Windows | [code.visualstudio.com](https://code.visualstudio.com/download) — instalador `.exe` |

**2. Instalar as extensões necessárias**

Abra o VS Code e instale pelo painel de extensões (`Ctrl+Shift+X`):

| Extensão | ID | Para que serve |
|----------|----|---------------|
| Python | `ms-python.python` | Suporte a Python |
| Jupyter | `ms-toolsai.jupyter` | Abrir e executar `.ipynb` |

Ou instale via terminal:

```bash
code --install-extension ms-python.python
code --install-extension ms-toolsai.jupyter
```

**3. Abrir o projeto**

```bash
cd spark-delta-minio-despesa
code .
```

**4. Selecionar o kernel `.venv`**

- Abra qualquer notebook (ex: `notebook/00_setup_sqlserver.ipynb`)
- No canto superior direito clique em **"Select Kernel"**
- Escolha **"Python Environments..."** → selecione `.venv`

> Se o `.venv` não aparecer, pressione `Ctrl+Shift+P` → `Python: Select Interpreter` → aponte para `.venv/bin/python` (Linux) ou `.venv\Scripts\python.exe` (Windows).

**5. Executar as células**

- `Shift+Enter` — executa a célula atual e avança
- `Ctrl+Enter` — executa a célula atual e permanece
- `Ctrl+Shift+P` → `Notebook: Run All` — executa todas as células

Não é necessário abrir navegador nem lidar com token.

---

## Executar os Notebooks

Execute **em ordem**, com o kernel `.venv` selecionado:

| # | Notebook | O que faz |
|---|----------|-----------|
| 0 | `00_setup_sqlserver.ipynb` | Cria o banco, as 11 tabelas e carrega os CSVs via JDBC |
| 1 | `01_sqlserver_to_minio_csv.ipynb` | Extrai SQL Server → CSV no MinIO `landing-zone` |
| 2 | `02_csv_to_delta.ipynb` | Converte CSV `landing-zone` → Delta Lake `bronze` |
| 3 | `03_dml_delta.ipynb` | INSERT · UPDATE · DELETE + HISTORY + TIME TRAVEL |

---

## Estrutura do Projeto

```
spark-delta-minio-despesa/
├── docker-compose.yml                    # SQL Server 2022 + MinIO
├── .env.example                          # Template de variáveis
├── .env                                  # Variáveis (não versionado)
├── pyproject.toml                        # Dependências Python (UV)
├── .python-version                       # Python 3.11
├── mkdocs.yml                            # Documentação MkDocs
├── README.md
├── data/                                 # CSVs do dataset
│   ├── orgao.csv
│   ├── unidade.csv
│   ├── programa.csv
│   ├── acao.csv
│   ├── fonte_recurso.csv
│   ├── credor.csv
│   ├── empenho.csv
│   ├── item_empenho.csv
│   ├── liquidacao.csv
│   ├── pagamento.csv
│   └── historico_preco.csv
└── notebook/
    ├── 00_setup_sqlserver.ipynb
    ├── 01_sqlserver_to_minio_csv.ipynb
    ├── 02_csv_to_delta.ipynb
    └── 03_dml_delta.ipynb
```

---

## Credenciais padrão

| Serviço | Usuário | Senha |
|---------|---------|-------|
| SQL Server | `sa` | `SqlServer@2024!` |
| MinIO | `minioadmin` | `minioadmin` |

---

## Tecnologias

| Tecnologia | Versão | Papel |
|------------|--------|-------|
| Apache Spark (PySpark) | 3.5.3 | Processamento distribuído |
| Delta Lake | 3.2.0 | Formato lakehouse com ACID |
| MinIO | Feb 2025 | Object Storage (S3-compatible) |
| SQL Server | 2022 Developer | Banco de dados fonte |
| Docker Compose | v2 | Orquestração |
| Python | 3.11 | Linguagem principal |
| UV | latest | Gerenciador de pacotes |

---

## Tabelas Gerenciadas vs Não Gerenciadas

Este projeto usa **Tabelas Não Gerenciadas** no Delta Lake:

| | Gerenciada | Não Gerenciada |
|-|------------|----------------|
| **Criação** | `df.write.saveAsTable(...)` | `df.write.save('s3a://...')` |
| **Dados** | Gerenciados pelo Spark | MinIO / S3 |
| **`DROP TABLE`** | Remove dados + metadados | Remove **só metadados** |
| **Uso ideal** | ETL local, temporários | Data Lakes em cloud |

---

## Referências

- [Delta Lake — Documentação](https://docs.delta.io/latest/)
- [MinIO — Documentação](https://min.io/docs/minio/linux/index.html)
- [SQL Server 2022 — Docker](https://learn.microsoft.com/sql/linux/quickstart-install-connect-docker)
- [PySpark — Documentação](https://spark.apache.org/docs/latest/api/python/)
- [Portal de Dados Abertos — Despesa Pública](https://dados.gov.br/dados/conjuntos-dados/administracao---despesa-publica---empenhos)
- [Repositório Modelo do Professor](https://github.com/jlsilva01/spark-delta-minio-sqlserver)
