# Instagram JSON Collector — Radar Ódio

Coletor incremental de perfis, mídias, comentários e stories do Instagram. Os
dados são persistidos diretamente em arquivos JSON UTF-8, organizados por
usuário e mídia, sem dependência de Kafka.

## Pré-requisitos

- Python 3.10 ou superior; ou
- Docker e Docker Compose.

Crie um `.env` na raiz:

```env
IG_USERNAME=seu_usuario
IG_PASSWORD=sua_senha
```

A sessão autenticada é reutilizada pelo arquivo `session.json`. O `.env`, a
sessão e a pasta `data/` estão ignorados pelo Git.

## Alvos

O coletor lê `users.csv` por padrão. Use um perfil por linha, com ou sem o
cabeçalho `username`:

```csv
username
usuario1
usuario2
```

## Execução local

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install -r SOURCE/requirements.txt
python SOURCE/main.py --once
```

Sem `--once`, o processo repete a coleta a cada hora:

```powershell
python SOURCE/main.py
```

Opções principais:

```text
--csv CAMINHO             CSV de usuários (padrão: users.csv)
--output DIRETORIO        raiz dos JSONs (padrão: data)
--media-limit N           mídias recentes por usuário (padrão: 10)
--stories-limit N         stories por usuário (padrão: 10)
--comments-limit N        comentários por mídia (padrão: 30)
--delay-min SEGUNDOS      pausa mínima entre chamadas (padrão: 8)
--delay-max SEGUNDOS      pausa máxima entre chamadas (padrão: 40)
--cycle-interval SEGUNDOS intervalo entre ciclos (padrão: 3600)
--once                    executa um único ciclo
```

Exemplo:

```powershell
python SOURCE/main.py `
  --csv users.csv `
  --output data `
  --media-limit 20 `
  --stories-limit 10 `
  --comments-limit 50 `
  --once
```

## Estrutura dos dados

```text
data/
└── users/
    └── {user_id}/
        ├── user.json
        ├── observations.json
        ├── stories/
        │   └── {story_id}.json
        └── media/
            └── {media_id}/
                ├── media.json
                ├── comments.json
                └── observations.json
```

- `user.json`: estado mais recente do perfil, com `collected_at`.
- `observations.json` do usuário: série temporal de seguidores, contas seguidas
  e quantidade de mídias.
- `media.json`: estado mais recente da publicação, com `collected_at`.
- `comments.json`: lista acumulada, deduplicada pelo ID estável do comentário.
- `observations.json` da mídia: série temporal de curtidas, comentários,
  visualizações e reproduções.
- `stories/{story_id}.json`: um arquivo imutável para cada story encontrado.

Datas são gravadas em ISO 8601 e os arquivos usam `ensure_ascii=False`,
preservando acentos e emojis. A gravação é atômica: cada JSON é escrito em um
arquivo temporário e substituído somente depois de concluído.

### Coleta incremental

A cada ciclo, o coletor reobserva o perfil e as mídias recentes para preservar
a série temporal. Comentários só são consultados quando a contagem da mídia
aumenta; o resultado é mesclado ao `comments.json` sem duplicar IDs. Stories já
salvos não são sobrescritos.

## Docker

O volume `data/` persiste tanto a sessão quanto os JSONs coletados:

```powershell
docker compose up -d --build
docker compose logs -f app
docker compose down
```

Variáveis de caminhos opcionais:

```env
USERS_CSV_PATH=C:/dados/instagram/users.csv
SESSION_DIR_PATH=C:/dados/instagram
```

No container, `SESSION_DIR_PATH` é montado em `/app/data`; portanto os arquivos
ficam em `SESSION_DIR_PATH/users/` e a sessão em
`SESSION_DIR_PATH/session.json`.

## Produção

Defina `APP_IMAGE`, credenciais e volumes em um arquivo fora do repositório:

```env
APP_IMAGE=seuusuario/instagram-coletor:1.0.0
IG_USERNAME=usuario_coletor
IG_PASSWORD=senha_do_usuario
USERS_CSV_PATH=/opt/instagram/users.csv
SESSION_DIR_PATH=/var/lib/instagram
```

Depois execute:

```bash
docker compose -f docker-compose.prod.yml --env-file /etc/instagram/production.env pull
docker compose -f docker-compose.prod.yml --env-file /etc/instagram/production.env up -d
```

## Testes

```powershell
Set-Location SOURCE
python -m unittest discover -v
```
