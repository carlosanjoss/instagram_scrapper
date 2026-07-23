# Instagram Kafka Collector

Aplicação Python que agenda coletas de perfis do Instagram e processa as tarefas por meio do Kafka. O consumidor utiliza retry, offsets transacionais e controle de idempotência por `task_id`.

## Pré-requisitos

Para executar localmente:

- Python 3.10 ou superior
- Kafka acessível pela aplicação

Para executar com containers:

- Docker
- Docker Compose

## Configuração

Crie um arquivo `.env` na raiz do projeto:

```env
IG_USERNAME=seu_usuario
IG_PASSWORD=sua_senha
```

O `.env` e o arquivo de sessão não devem ser versionados.

### Arquivo de usuários

Por padrão, a aplicação lê `users.csv` na raiz. Informe um usuário por linha, sem necessidade de cabeçalho:

```csv
usuario1
usuario2
usuario3
```

## Execução com Python

Crie e ative o ambiente virtual.

No PowerShell:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

No Git Bash:

```bash
python -m venv venv
source venv/Scripts/activate
python -m pip install -r requirements.txt
```

Suba o Kafka fornecido pelo projeto:

```bash
docker compose -f kafka/docker-compose.yml up -d
```

Execute a aplicação:

```bash
python main.py
```

O padrão para uma execução local é:

- CSV: `users.csv`
- Sessão: `session.json`
- Kafka: `localhost:29092`
- Novo ciclo: a cada 3600 segundos

### Caminhos personalizados com Python

Para escolher outro CSV:

```bash
python main.py --csv "C:/dados/instagram/users.csv"
```

No PowerShell, configure o arquivo da sessão assim:

```powershell
$env:IG_SESSION_FILE = "C:\dados\instagram\session.json"
python main.py --csv "C:\dados\instagram\users.csv"
```

No Git Bash:

```bash
export IG_SESSION_FILE="C:/dados/instagram/session.json"
python main.py --csv "C:/dados/instagram/users.csv"
```

Para usar outro servidor Kafka:

```bash
python main.py --bootstrap-server "kafka.exemplo.com:9092"
```

Algumas configurações disponíveis:

```bash
python main.py \
  --csv users.csv \
  --bootstrap-server localhost:29092 \
  --cycle-interval 3600 \
  --media-limit 10 \
  --stories-limit 10 \
  --comments-limit 30 \
  --delay-min 8 \
  --delay-max 40
```

Use `python main.py --help` para consultar todas as opções.

## Execução com Docker

Kafka e aplicação possuem Composes separados. O Compose padrão constrói a aplicação localmente e é indicado para desenvolvimento. Inicie primeiro o Kafka:

```bash
docker compose -f kafka/docker-compose.yml up -d
```

Depois construa e inicie a aplicação:

```bash
docker compose up -d --build
```

Consulte os logs:

```bash
docker compose logs -f app
```

O Kafka UI fica disponível em:

```text
http://localhost:8081
```

### Caminhos personalizados com Docker

Adicione ao `.env` os caminhos do host:

```env
IG_USERNAME=seu_usuario
IG_PASSWORD=sua_senha
KAFKA_BOOTSTRAP_SERVER=kafka:9092
USERS_CSV_PATH=C:/dados/instagram/users.csv
SESSION_DIR_PATH=C:/dados/instagram/session
```

Nesse exemplo, a sessão será persistida em:

```text
C:/dados/instagram/session/session.json
```

Se as variáveis não forem informadas, serão utilizados:

- CSV: `./users.csv`
- Diretório da sessão: `./data`
- Arquivo da sessão: `./data/session.json`

Em Linux, os caminhos podem ser definidos desta forma:

```env
USERS_CSV_PATH=/opt/instagram/users.csv
SESSION_DIR_PATH=/opt/instagram/session
```

Os dois Composes compartilham a rede Docker `instagram-network`. Por isso, o Compose do Kafka deve ser iniciado antes do Compose da aplicação.

## Imagem de produção

O `Dockerfile` constrói a imagem da aplicação. Em produção, prefira construir e publicar essa imagem no pipeline de CI/CD; o servidor deve apenas baixá-la e executá-la.

Construa uma versão localmente ou no pipeline:

```bash
docker build -t seuusuario/instagram-coletor:1.0.0 .
```

Autentique-se e publique no Docker Hub:

```bash
docker login
docker push seuusuario/instagram-coletor:1.0.0
```

Não reutilize a mesma tag para versões diferentes. Prefira tags imutáveis como `1.0.0` ou o hash do commit.

### Injeção de variáveis em produção

O Compose declara explicitamente as variáveis entregues ao container. `IG_USERNAME` e `IG_PASSWORD` são obrigatórias; a inicialização falhará antes de criar o container caso estejam ausentes.

Mantenha o arquivo de produção fora do repositório, por exemplo:

```text
/etc/instagram-api/production.env
```

Conteúdo sugerido:

```env
APP_IMAGE=seuusuario/instagram-api:1.0.0
IG_USERNAME=usuario_coletor
IG_PASSWORD=senha_do_usuario
KAFKA_BOOTSTRAP_SERVER=kafka.interno:9092
USERS_CSV_PATH=/opt/instagram/config/users.csv
SESSION_DIR_PATH=/var/lib/instagram/session
```

Proteja o arquivo no servidor:

```bash
sudo chown root:root /etc/instagram-api/production.env
sudo chmod 600 /etc/instagram-api/production.env
```

O Compose de produção não cria Kafka e não exige uma rede Docker externa. `KAFKA_BOOTSTRAP_SERVER` deve apontar para um endereço que seja alcançável de dentro do container, como um DNS interno ou IP do servidor Kafka. O endereço anunciado pelo broker em `advertised.listeners` também precisa ser alcançável pelo container.

Baixe a imagem versionada indicada por `APP_IMAGE`:

```bash
docker compose \
  -f docker-compose.prod.yml \
  --env-file /etc/instagram-api/production.env \
  pull
```

Inicie o container de produção sem realizar build no servidor:

```bash
docker compose \
  -f docker-compose.prod.yml \
  --env-file /etc/instagram-api/production.env \
  up -d
```

O parâmetro `--env-file` fornece valores para a interpolação do Compose. Apenas `IG_USERNAME`, `IG_PASSWORD` e `IG_SESSION_FILE` são inseridas como variáveis no container; a imagem, os caminhos do host, a rede e o endereço Kafka configuram o deployment.

Para verificar a configuração resolvida sem iniciar containers:

```bash
docker compose \
  -f docker-compose.prod.yml \
  --env-file /etc/instagram-api/production.env \
  config
```

Para atualizar a aplicação após publicar uma nova tag, altere `APP_IMAGE` no arquivo de produção e execute:

```bash
docker compose \
  -f docker-compose.prod.yml \
  --env-file /etc/instagram-api/production.env \
  pull

docker compose \
  -f docker-compose.prod.yml \
  --env-file /etc/instagram-api/production.env \
  up -d
```

## Encerramento

Pare somente a aplicação:

```bash
docker compose down
```

Pare Kafka e Kafka UI sem excluir os dados:

```bash
docker compose -f kafka/docker-compose.yml down
```

Os dados do Kafka permanecem no volume `kafka-data`. Não use `down -v` se desejar preservá-los.

## Tópicos Kafka

O Compose do Kafka cria automaticamente:

```text
instagram.tasks
instagram.tasks.processed
instagram.user.data
instagram.media.data
instagram.comments.data
instagram.stories.data
instagram.user.latest
instagram.media.comments.latest
instagram.user.stories.latest
instagram.user.observations
instagram.media.observations
```

Os tópicos `*.latest` e `instagram.tasks.processed` utilizam compactação. Em um Kafka externo, esses tópicos precisam ser provisionados pela infraestrutura do servidor.

## Série temporal de distribuição

A cada ciclo, o coletor consulta novamente as mídias recentes e grava eventos
imutáveis em `instagram.user.observations` e
`instagram.media.observations`. As observações incluem horário da coleta, idade
da publicação, seguidores, curtidas, comentários, visualizações e reproduções.

Os campos `reach`, `impressions`, `non_follower_reach` e
`hashtag_visible` ficam nulos quando a fonte não oferece essas métricas. Os
tópicos locais têm retenção de 180 dias; em produção, envie-os também para um
armazenamento analítico permanente.

O número de posts reobservados é controlado por `--media-limit`. Esses dados
permitem estimar redução de distribuição, mas não confirmar diretamente um
shadowban.
