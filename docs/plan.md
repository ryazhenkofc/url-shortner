# URL Shortener: план реализации

Реализуем дизайн из SIS #1 (T1. URL Shortener): API, схему БД, двухуровневый кэш, защиту от перегрузки. Всё поднимается одной командой `docker compose up`.

---

## 0. Что делаем и чего не делаем

**В объёме:**
- `POST /api/v1/links`: создать ссылку (случайный код или свой алиас), `Idempotency-Key`.
- `GET /{code}`: редирект 302, а также 404 и 410.
- `GET /api/v1/links/{code}`: информация о ссылке (только владелец).
- `DELETE /api/v1/links/{code}`: soft delete и инвалидация кэша.
- L1 (in-process) и L2 (Redis) кэш, single-flight, negative cache.
- PostgreSQL primary и replica: запись в primary, чтение с реплики с перепроверкой на primary.
- Redis primary, replica и Sentinel.
- L7-балансировщик nginx перед 3 app-нодами.
- Таймауты, semaphore на каждую зависимость, ограничение конкурентности, ответ 503 или 429 с `Retry-After`.
- Метрики Prometheus, нагрузочный тест k6, сценарии отказов.

**Вне объёма (как в дизайне):**
- Аналитика кликов.
- QR-коды.
- Мультирегион.
- Kafka.
- Автоматический failover PostgreSQL (Patroni): в локальной среде показываем ручной promote.
- TLS локально: в проде его снимает балансировщик.

---

## 1. Стек

| Слой | Выбор | Зачем |
| --- | --- | --- |
| Язык и фреймворк | Python 3.12, FastAPI, uvicorn (uvloop, httptools) | async I/O: на одной ноде тысячи одновременных запросов без потоков |
| PostgreSQL-драйвер | `asyncpg` | самый быстрый async-драйвер, пулы соединений |
| Redis-клиент | `redis` (`redis.asyncio`, Sentinel) | обнаружение primary через Sentinel |
| L1-кэш | `cachetools.TTLCache` | аналог Caffeine: размер и TTL |
| Конфиг | `pydantic-settings` | всё задаётся через env |
| Метрики | `prometheus-client` | `/metrics` на каждой ноде |
| Балансировщик | nginx 1.27 | L7, `least_conn`, rate limit, повтор GET |
| БД | `postgres:17` | streaming-репликация через `pg_basebackup` |
| Кэш | `redis:7.4-alpine` | primary, replica и Sentinel |
| Нагрузка | `grafana/k6` | constant-arrival-rate, пороги p99 |
| Тесты | `pytest`, `pytest-asyncio`, `httpx` | unit и интеграционные тесты против compose |

**Соответствие дизайну:**

| В дизайне | В коде |
| --- | --- |
| L1 Caffeine 10k, 10 s | `TTLCache(maxsize=10_000, ttl=10)` |
| semaphore per downstream | `asyncio.Semaphore` с таймаутом захвата 50 мс |
| bounded queue → 503 | `uvicorn --limit-concurrency 1000` (сверх лимита ответ 503) |
| single-flight | словарь `code → asyncio.Future` |
| pub/sub → L1 | фоновая задача подписывается на канал `links:inv` |
| Patroni | вне локального объёма, демонстрируем ручной `pg_ctl promote` |

---

## 2. Структура репозитория

```
url-shortener/
├── app/
│   ├── main.py            # FastAPI app, lifespan: пулы, фоновые задачи
│   ├── config.py          # Settings из env
│   ├── db.py              # пулы asyncpg: primary и replica
│   ├── redis_client.py    # Sentinel → master, таймауты
│   ├── cache.py           # L1, L2, single-flight, negative cache, Entry
│   ├── guard.py           # Semaphore с таймаутом → Overloaded
│   ├── codegen.py         # base62, валидация алиаса и URL
│   ├── repo.py            # SQL: create, get, delete, idempotency
│   ├── auth.py            # Bearer → user_id
│   ├── invalidation.py    # publish и subscribe links:inv
│   ├── jobs.py            # очистка idempotency_keys (advisory lock)
│   ├── metrics.py         # счётчики и гистограммы
│   ├── errors.py          # исключения → HTTP-коды
│   └── routes/
│       ├── redirect.py    # GET /{code}
│       ├── links.py       # /api/v1/links
│       └── health.py      # /health/live, /health/ready
├── migrations/
│   └── 001_init.sql
├── deploy/
│   ├── nginx/nginx.conf
│   ├── postgres/00-replication.sh
│   └── prometheus/prometheus.yml
├── load/
│   ├── redirect.js        # k6
│   └── seed.sql           # 100k ссылок для нагрузки
├── tests/
│   ├── unit/
│   └── integration/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── Makefile
└── README.md
```

---

## 3. Схема БД (`migrations/001_init.sql`)

Добавлена одна таблица к дизайну, `api_keys`: Bearer-токен нужно где-то проверять. В БД хранится только хеш ключа.

```sql
CREATE TABLE users (
    id          BIGSERIAL PRIMARY KEY,
    email       TEXT UNIQUE NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
    key_hash    BYTEA PRIMARY KEY,                 -- sha256(key)
    user_id     BIGINT NOT NULL REFERENCES users(id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE links (
    code        VARCHAR(10) PRIMARY KEY,           -- base62, 7 символов, или алиас
    long_url    TEXT NOT NULL,
    owner_id    BIGINT REFERENCES users(id),       -- NULL = анонимная
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ,                       -- NULL = бессрочно
    deleted_at  TIMESTAMPTZ                        -- NULL = активна
);
CREATE INDEX idx_links_owner   ON links (owner_id, created_at DESC);
CREATE INDEX idx_links_expires ON links (expires_at) WHERE expires_at IS NOT NULL;

CREATE TABLE idempotency_keys (
    key              VARCHAR(64) PRIMARY KEY,
    code             VARCHAR(10) NOT NULL,
    response_status  SMALLINT NOT NULL,
    response_body    JSONB NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX idx_idem_created ON idempotency_keys (created_at);

-- dev-данные: пользователь и ключ "dev-key-123"
INSERT INTO users (email) VALUES ('demo@example.com');
INSERT INTO api_keys (key_hash, user_id)
VALUES (sha256('dev-key-123'::bytea), 1);
```

Миграция лежит в `/docker-entrypoint-initdb.d` у primary и выполняется один раз при первой инициализации тома. Реплика получает её через `pg_basebackup`. Для прода нужен инструмент миграций (alembic или dbmate), это отмечено в разделе 11.

---

## 4. API (контракт)

| Метод и путь | Auth | Успех | Ошибки |
| --- | --- | --- | --- |
| `POST /api/v1/links` | Bearer | `201 {code, short_url, long_url, expires_at}` | 400 URL/алиас, 401, 409 алиас занят, 429, 503 + `Retry-After` |
| `GET /{code}` | нет | `302 Location: long_url` | 404 нет кода, 410 истекла или удалена, 503 |
| `GET /api/v1/links/{code}` | Bearer | `200 {code, long_url, created_at, expires_at}` | 401, 403 не владелец, 404 |
| `DELETE /api/v1/links/{code}` | Bearer | `204` | 401, 403, 404 (в том числе уже удалена) |
| `GET /health/live` | нет | 200, если процесс жив | |
| `GET /health/ready` | нет | 200, если доступна хотя бы одна БД | 503 |
| `GET /metrics` | нет (только внутренняя сеть, через nginx не отдаётся) | Prometheus | |

**Правила:**
- `Idempotency-Key`: необязательный заголовок, до 64 символов (uuid). С ним повтор `POST` возвращает сохранённый 201.
- Алиас: `^[A-Za-z0-9_-]{3,10}$`. Зарезервированы: `api`, `admin`, `health`, `metrics`, `static`.
- URL: только `http` или `https`, непустой хост, до 2048 символов. Не наш собственный домен (защита от петель). Хост не из blocklist (список в env).
- Редирект всегда 302 с `Cache-Control: private, max-age=0`, чтобы браузер не закрепил удалённую ссылку.

**Примеры:**

```bash
curl -s -X POST localhost:8080/api/v1/links \
  -H 'Authorization: Bearer dev-key-123' \
  -H "Idempotency-Key: $(uuidgen)" \
  -H 'Content-Type: application/json' \
  -d '{"long_url":"https://example.com/very/long/path"}'

curl -si localhost:8080/aB3xK9q | head -3          # 302 + Location
curl -si -X DELETE localhost:8080/api/v1/links/aB3xK9q -H 'Authorization: Bearer dev-key-123'
```

---

## 5. Ключевые части кода

### 5.1 Конфиг (`config.py`)

```python
class Settings(BaseSettings):
    node_id: str = "local"
    base_url: str = "http://localhost:8080"
    pg_primary_dsn: str
    pg_replica_dsn: str
    redis_sentinels: str = ""          # "host:port,host:port"; пусто → redis_url
    redis_master_name: str = "mymaster"
    redis_url: str = "redis://redis-primary:6379/0"

    redis_timeout_s: float = 0.05      # 50 мс → идём в БД
    db_read_timeout_s: float = 0.1     # чтение на пути редиректа
    pool_size: int = 20
    redis_concurrency: int = 40
    acquire_timeout_s: float = 0.05

    l1_size: int = 10_000
    l1_ttl_s: int = 10
    l2_ttl_s: int = 86_400             # ±10% джиттер
    l2_negative_ttl_s: int = 60
    url_blocklist: str = ""
```

### 5.2 Пулы БД (`db.py`)

```python
SERVER_SETTINGS = {
    "statement_timeout": "5000",
    "idle_in_transaction_session_timeout": "30000",
}

async def make_pools(s: Settings):
    primary = await asyncpg.create_pool(s.pg_primary_dsn, min_size=2, max_size=s.pool_size,
                                        server_settings=SERVER_SETTINGS, timeout=2)
    replica = await asyncpg.create_pool(s.pg_replica_dsn, min_size=2, max_size=s.pool_size,
                                        server_settings=SERVER_SETTINGS, timeout=2)
    return primary, replica
```

При старте ноды пулы создаются с повторами и backoff: БД может подниматься дольше приложения.

### 5.3 Guard: semaphore и перегрузка (`guard.py`)

```python
class Overloaded(Exception): ...

class Guard:
    def __init__(self, limit: int, acquire_timeout: float):
        self._sem = asyncio.Semaphore(limit)
        self._t = acquire_timeout

    @asynccontextmanager
    async def slot(self):
        try:
            await asyncio.wait_for(self._sem.acquire(), self._t)
        except asyncio.TimeoutError:
            OVERLOAD_REJECTS.inc()
            raise Overloaded()
        try:
            yield
        finally:
            self._sem.release()

db_guard = Guard(settings.pool_size, settings.acquire_timeout_s)        # = размер пула
redis_guard = Guard(settings.redis_concurrency, settings.acquire_timeout_s)
```

Обработчик `Overloaded` отвечает `503` с заголовком `Retry-After: 1`.

### 5.4 Кэш: L1, L2, single-flight (`cache.py`)

```python
@dataclass(frozen=True)
class Entry:
    kind: Literal["ok", "missing", "gone"]
    url: str | None = None
    expires_at: datetime | None = None

class SingleFlight:
    def __init__(self):
        self._inflight: dict[str, asyncio.Future] = {}

    async def do(self, key, fn):
        if (fut := self._inflight.get(key)) is not None:
            return await asyncio.shield(fut)          # ждём результат первого
        fut = asyncio.get_running_loop().create_future()
        self._inflight[key] = fut
        try:
            res = await fn()
            fut.set_result(res)
            return res
        except Exception as e:
            fut.set_exception(e)
            fut.exception()                           # чтобы не было warning без ожидающих
            raise
        finally:
            self._inflight.pop(key, None)

class Resolver:
    async def resolve(self, code: str) -> Entry:
        if (e := self.l1.get(code)) is not None:
            HITS.labels("l1").inc()
            return e
        return await self.sf.do(code, lambda: self._load(code))

    async def _load(self, code: str) -> Entry:
        e = await self._redis_get(code)               # None при промахе ИЛИ ошибке/таймауте Redis
        if e is not None:
            HITS.labels("l2").inc()
        else:
            HITS.labels("db").inc()
            e = await self._db_get(code)
            await self._redis_set(code, e)            # best effort
        self.l1[code] = e
        return e

    async def _db_get(self, code: str) -> Entry:
        row = await self._read(self.replica, code)    # при ошибке реплики → primary
        if row is None:
            row = await self._read(self.primary, code)  # реплика могла отстать
        return to_entry(row)                          # None → missing; deleted_at → gone
```

**Redis:**
- Ключ `u:{code}`, значение JSON `{"u": url, "e": expires_at}`, либо `"!missing"` или `"!gone"`.
- TTL для `ok`: `min(86400 × uniform(0.9, 1.1), expires_at − now)`. Для `missing` 60 с, для `gone` 300 с.
- Каждый вызов идёт через `redis_guard.slot()` и `asyncio.wait_for(..., 0.05)`. Любая ошибка означает «промах», и запрос идёт дальше в БД.

### 5.5 Редирект (`routes/redirect.py`)

```python
CODE_RE = re.compile(r"^[A-Za-z0-9_-]{3,10}$")

@router.get("/{code}")
async def redirect(code: str):
    if not CODE_RE.fullmatch(code):
        raise HTTPException(404)
    e = await resolver.resolve(code)
    if e.kind == "missing":
        raise HTTPException(404)
    if e.kind == "gone" or (e.expires_at and e.expires_at <= utcnow()):
        raise HTTPException(410)
    return RedirectResponse(e.url, status_code=302,
                            headers={"Cache-Control": "private, max-age=0"})
```

Роутеры `/api` и `/health` подключаются раньше. `{code}` не совпадает с путями, в которых есть `/`.

### 5.6 Создание (`repo.py`)

```python
INSERT_LINK = """
INSERT INTO links (code, long_url, owner_id, expires_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (code) DO NOTHING
RETURNING created_at"""

INSERT_IDEM = """
INSERT INTO idempotency_keys (key, code, response_status, response_body)
VALUES ($1, $2, 201, $3::jsonb)
ON CONFLICT (key) DO NOTHING
RETURNING 1"""

async def create_link(req, user_id, idem_key):
    if idem_key and (stored := await get_idem(idem_key)):        # быстрый путь повтора
        return stored

    async def txn():
        async with db_guard.slot(), primary.acquire() as conn, conn.transaction():
            for _ in range(1 if req.alias else 3):
                code = req.alias or new_code()
                row = await conn.fetchrow(INSERT_LINK, code, req.long_url, user_id, req.expires_at)
                if row:
                    break
            else:
                raise AliasTaken() if req.alias else Overloaded()   # 409 / 503
            body = make_body(code, req, row["created_at"])
            if idem_key and not await conn.fetchval(INSERT_IDEM, idem_key, code, json.dumps(body)):
                raise IdemRace()            # другой запрос с тем же ключом успел первым → откат
            return code, body

    try:
        code, body = await retry_on(txn, (SerializationError, DeadlockDetectedError), attempts=3)
    except IdemRace:
        return await get_idem(idem_key)     # вернуть его сохранённый ответ
    await invalidate(code)                  # сбросить возможный "missing" в Redis и L1
    return 201, body
```

Почему это корректно:
- Уникальность кода гарантирует `PRIMARY KEY`, а не предварительная проверка через SELECT.
- Две ноды с одинаковым кодом: одна вставит строку, вторая получит 0 строк и возьмёт новый код.
- Два запроса с одним ключом: второй ждёт коммита первого, получает `DO NOTHING`, откатывает свою ссылку и возвращает ответ первого.

### 5.7 Генерация кода и валидация (`codegen.py`)

```python
ALPHABET = string.ascii_uppercase + string.ascii_lowercase + string.digits   # 62
ALIAS_RE = re.compile(r"^[A-Za-z0-9_-]{3,10}$")
RESERVED = {"api", "admin", "health", "metrics", "static"}

def new_code(n: int = 7) -> str:
    return "".join(secrets.choice(ALPHABET) for _ in range(n))   # равномерно, криптостойко

def validate_url(url: str, own_host: str, blocklist: set[str]) -> None:
    if len(url) > 2048: raise BadRequest("url too long")
    p = urlsplit(url)
    if p.scheme not in ("http", "https") or not p.hostname: raise BadRequest("bad url")
    if p.hostname == own_host or p.hostname in blocklist: raise BadRequest("host not allowed")
```

### 5.8 Удаление и инвалидация (`repo.py`, `invalidation.py`)

```sql
UPDATE links SET deleted_at = now()
WHERE code = $1 AND owner_id = $2 AND deleted_at IS NULL
RETURNING code;
```

Если обновлено 0 строк, читаем строку и различаем случаи: 404 (нет или уже удалена) и 403 (чужая).

```python
async def invalidate(code: str):
    l1.pop(code, None)
    with suppress(Exception):                    # Redis лёг → спасёт TTL
        await redis.delete(f"u:{code}")
        await redis.publish("links:inv", code)

async def listen_invalidations():                # фоновая задача на каждой ноде
    while True:
        try:
            async with redis.pubsub() as ps:
                await ps.subscribe("links:inv")
                l1.clear()                       # после (пере)подключения: могли пропустить сообщения
                async for msg in ps.listen():
                    if msg["type"] == "message":
                        l1.pop(msg["data"], None)
        except Exception:
            await asyncio.sleep(1)               # backoff, затем переподключение
```

Порядок такой: сначала COMMIT в БД, потом `DEL` в Redis, потом publish.

### 5.9 Авторизация (`auth.py`)

`Authorization: Bearer <key>` → `sha256(key)` → `SELECT user_id FROM api_keys WHERE key_hash = $1`. Результат кэшируется в L1 на 60 секунд. Нет ключа или он неверный → 401.

### 5.10 Фоновая очистка (`jobs.py`)

Раз в 10 минут на каждой ноде, но выполняется только на одной:

```sql
BEGIN;
SELECT pg_try_advisory_xact_lock(4242);          -- false → другая нода уже чистит, выходим
DELETE FROM idempotency_keys WHERE created_at < now() - interval '24 hours';
COMMIT;
```

### 5.11 Health, ошибки, метрики

- `/health/live`: всегда 200.
- `/health/ready`: `SELECT 1` на primary или replica с таймаутом 200 мс. Redis не проверяется: без него сервис работает в деградированном режиме.
- Маппинг ошибок: `Overloaded` и ошибки соединения с БД (`asyncpg.PostgresConnectionError`, `OSError`, `asyncio.TimeoutError`) → 503 + `Retry-After: 1`. `AliasTaken` → 409. `BadRequest` → 400.
- Метрики:
  - `http_request_duration_seconds{route,status}`;
  - `cache_hits_total{layer=l1|l2|db}`;
  - `overload_rejects_total`;
  - `db_errors_total{pool}`;
  - `redis_errors_total`.

---

## 6. Docker

### 6.1 `requirements.txt`

```
fastapi
uvicorn[standard]
asyncpg
redis>=5
cachetools
pydantic-settings
prometheus-client
```

Версии фиксируются при первой установке: `pip freeze > requirements.lock`, и в Dockerfile ставится lock-файл.

### 6.2 `Dockerfile`

```dockerfile
FROM python:3.12-slim AS deps
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

FROM python:3.12-slim
RUN useradd --system --uid 10001 app
WORKDIR /app
COPY --from=deps /install /usr/local
COPY app ./app
USER app
ENV PYTHONUNBUFFERED=1
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=2s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/health/live', timeout=1)"
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", \
     "--loop", "uvloop", "--http", "httptools", \
     "--limit-concurrency", "1000", "--timeout-graceful-shutdown", "10", \
     "--no-access-log"]
```

`--limit-concurrency 1000` это и есть bounded queue: сверх лимита uvicorn сразу отвечает 503.

### 6.3 `deploy/postgres/00-replication.sh`

```bash
#!/bin/bash
set -e
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<SQL
CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD '${REPL_PASSWORD}';
SQL
echo "host replication replicator all scram-sha-256" >> "$PGDATA/pg_hba.conf"
```

### 6.4 `deploy/nginx/nginx.conf`

```nginx
events { worker_connections 4096; }

http {
    upstream app {
        least_conn;
        server app1:8000 max_fails=2 fail_timeout=10s;
        server app2:8000 max_fails=2 fail_timeout=10s;
        server app3:8000 max_fails=2 fail_timeout=10s;
        keepalive 64;
    }

    # Внутренние сети compose не лимитируются (нужно для k6). В проде убрать.
    geo $limit_ip {
        default        $binary_remote_addr;
        172.16.0.0/12  "";
    }
    limit_req_zone $limit_ip            zone=redir:10m rate=50r/s;
    limit_req_zone $http_authorization  zone=api:10m   rate=20r/s;
    limit_req_status 429;

    proxy_http_version 1.1;
    proxy_set_header Connection "";
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Request-Id $request_id;

    server {
        listen 80;

        location ^~ /api/ {
            limit_req zone=api burst=40 nodelay;
            proxy_connect_timeout 100ms;
            proxy_read_timeout 6s;
            proxy_next_upstream off;            # POST повторяет клиент с Idempotency-Key
            proxy_pass http://app;
        }

        location ^~ /health/ {
            proxy_pass http://app;
        }

        location ~ ^/[A-Za-z0-9_-]{3,10}$ {
            limit_req zone=redir burst=100 nodelay;
            proxy_connect_timeout 100ms;
            proxy_read_timeout 200ms;
            proxy_next_upstream error timeout http_502 http_503;
            proxy_next_upstream_tries 2;        # GET безопасно повторить на другой ноде
            proxy_pass http://app;
        }

        location = /metrics { return 404; }     # метрики только изнутри сети
        location / { return 404; }
    }
}
```

Пояснения:
- `location = /metrics` нужен, потому что слово `metrics` (7 символов) иначе совпадёт с регуляркой кода.
- В nginx open source нет активного health check, работает пассивный (`max_fails`). Активный есть в HAProxy и в облачных LB.

### 6.5 `docker-compose.yml`

```yaml
name: url-shortener

x-app: &app
  build: .
  restart: unless-stopped
  depends_on:
    pg-primary:     { condition: service_healthy }
    pg-replica:     { condition: service_healthy }
    redis-sentinel: { condition: service_started }

x-app-env: &app-env
  BASE_URL: http://localhost:8080
  PG_PRIMARY_DSN: postgresql://shortener:${PG_PASSWORD:-shortener}@pg-primary:5432/shortener
  PG_REPLICA_DSN: postgresql://shortener:${PG_PASSWORD:-shortener}@pg-replica:5432/shortener
  REDIS_SENTINELS: redis-sentinel:26379
  REDIS_MASTER_NAME: mymaster

services:
  nginx:
    image: nginx:1.27-alpine
    ports: ["8080:80"]
    volumes: ["./deploy/nginx/nginx.conf:/etc/nginx/nginx.conf:ro"]
    depends_on: [app1, app2, app3]

  app1: { <<: *app, environment: { <<: *app-env, NODE_ID: app1 } }
  app2: { <<: *app, environment: { <<: *app-env, NODE_ID: app2 } }
  app3: { <<: *app, environment: { <<: *app-env, NODE_ID: app3 } }

  pg-primary:
    image: postgres:17
    environment:
      POSTGRES_USER: shortener
      POSTGRES_PASSWORD: ${PG_PASSWORD:-shortener}
      POSTGRES_DB: shortener
      REPL_PASSWORD: ${REPL_PASSWORD:-replicator}
    volumes:
      - pg-primary:/var/lib/postgresql/data
      - ./deploy/postgres/00-replication.sh:/docker-entrypoint-initdb.d/00-replication.sh:ro
      - ./migrations/001_init.sql:/docker-entrypoint-initdb.d/01-init.sql:ro
    ports: ["5432:5432"]                     # для отладки, можно убрать
    healthcheck:
      # -h 127.0.0.1: TCP, а не unix-сокет, иначе "ready" сработает на временном сервере initdb
      test: ["CMD-SHELL", "pg_isready -h 127.0.0.1 -U shortener -d shortener"]
      interval: 3s
      retries: 30

  pg-replica:
    image: postgres:17
    user: postgres
    environment:
      PGPASSWORD: ${REPL_PASSWORD:-replicator}
    entrypoint: ["bash", "-c"]
    command:
      - |
        if [ ! -s "$$PGDATA/PG_VERSION" ]; then
          until pg_basebackup -h pg-primary -U replicator -D "$$PGDATA" -X stream -R; do
            echo "waiting for primary..."; rm -rf "$$PGDATA"/*; sleep 2
          done
          chmod 700 "$$PGDATA"
        fi
        exec postgres
    volumes: ["pg-replica:/var/lib/postgresql/data"]
    depends_on:
      pg-primary: { condition: service_healthy }
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -h 127.0.0.1 -U shortener -d shortener"]
      interval: 3s
      retries: 30

  redis-primary:
    image: redis:7.4-alpine
    command: ["redis-server", "--maxmemory", "256mb", "--maxmemory-policy", "allkeys-lfu",
              "--save", "", "--appendonly", "no"]
    healthcheck: { test: ["CMD", "redis-cli", "ping"], interval: 3s, retries: 20 }

  redis-replica:
    image: redis:7.4-alpine
    command: ["redis-server", "--replicaof", "redis-primary", "6379",
              "--maxmemory", "256mb", "--maxmemory-policy", "allkeys-lfu"]
    depends_on:
      redis-primary: { condition: service_healthy }

  redis-sentinel:
    image: redis:7.4-alpine
    command:
      - sh
      - -c
      - |
        cat > /tmp/sentinel.conf <<EOF
        port 26379
        sentinel resolve-hostnames yes
        sentinel announce-hostnames yes
        sentinel monitor mymaster redis-primary 6379 1
        sentinel down-after-milliseconds mymaster 5000
        sentinel failover-timeout mymaster 10000
        EOF
        exec redis-server /tmp/sentinel.conf --sentinel
    depends_on:
      redis-primary: { condition: service_healthy }
      redis-replica: { condition: service_started }

  # --- профиль observability: docker compose --profile obs up ---
  prometheus:
    image: prom/prometheus:v2.53.0
    profiles: ["obs"]
    volumes: ["./deploy/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro"]
    ports: ["9090:9090"]

  grafana:
    image: grafana/grafana:11.1.0
    profiles: ["obs"]
    ports: ["3000:3000"]

  # --- профиль load: docker compose --profile load run --rm k6 ---
  k6:
    image: grafana/k6:0.52.0
    profiles: ["load"]
    volumes: ["./load:/load:ro"]
    environment: { BASE: http://nginx }
    command: ["run", "/load/redirect.js"]

volumes:
  pg-primary:
  pg-replica:
```

Локальные размеры уменьшены по сравнению с продом: Redis 256 МБ вместо 6 ГБ, один Sentinel вместо трёх (кворум 1). В README это отмечено.

`deploy/prometheus/prometheus.yml`:

```yaml
scrape_configs:
  - job_name: app
    scrape_interval: 5s
    static_configs:
      - targets: ["app1:8000", "app2:8000", "app3:8000"]
```

### 6.6 `.env.example`

```
PG_PASSWORD=shortener
REPL_PASSWORD=replicator
```

### 6.7 `Makefile`

```make
up:        ; docker compose up -d --build
down:      ; docker compose down
reset:     ; docker compose down -v          # снести тома (новая инициализация БД)
logs:      ; docker compose logs -f app1 app2 app3
ps:        ; docker compose ps
test:      ; pytest tests/unit
test-int:  ; pytest tests/integration        # против поднятого compose
seed:      ; docker compose exec -T pg-primary psql -U shortener -d shortener < load/seed.sql
codes:     ; docker compose exec -T pg-replica psql -U shortener -d shortener -At \
               -c "SELECT json_agg(code) FROM links" > load/codes.json
load:      ; docker compose --profile load run --rm k6
obs:       ; docker compose --profile obs up -d
```

---

## 7. Этапы реализации

Каждый этап заканчивается рабочим `docker compose up` и проверкой из колонки «Готово, когда».

| # | Этап | Задачи | Готово, когда |
| --- | --- | --- | --- |
| M0 | Каркас | структура репо, `config.py`, `/health/live`, Dockerfile, compose (nginx, 3 ноды, pg primary и replica, redis) | `curl localhost:8080/health/live` → 200 с каждой ноды; на реплике `SELECT pg_is_in_recovery()` → `t` |
| M1 | Ядро без кэша | миграция, `codegen.py`, `auth.py`, POST (случайный код и алиас), `GET /{code}` из БД, GET info, ошибки 400/401/404/409 | создание и редирект работают; на алиас-дубль ответ 409 |
| M2 | Кэш | Redis через Sentinel, L1 `TTLCache`, single-flight, negative cache, TTL с джиттером и учётом `expires_at` | повторный редирект не трогает БД (метрика `cache_hits_total{layer}`); 100 параллельных запросов на холодный код → 1 запрос в БД |
| M3 | Запись и инвалидация | `Idempotency-Key`, DELETE, `invalidate()`, подписка `links:inv`, очистка `jobs.py` | повтор POST с тем же ключом → тот же код; после DELETE все 3 ноды отдают 410 не позже 10 с |
| M4 | Устойчивость | таймауты Redis и БД, `Guard`, `--limit-concurrency`, перепроверка на primary, fallback replica → primary, retry 40001/40P01, маппинг ошибок в 503 | сценарии отказов из раздела 9 проходят |
| M5 | Наблюдаемость и нагрузка | метрики, профиль `obs`, `seed.sql`, k6 | k6: 700 RPS, p99 < 50 мс, ошибок < 0.1% |
| M6 | Сдача | README (запуск, архитектура, решения, результаты k6 и отказов), диаграмма | новый человек поднимает всё по README за 5 минут |

---

## 8. Тесты

### Unit (`tests/unit`)

- `new_code()`: длина 7, все символы из алфавита. Распределение близко к равномерному (на 620k символов каждый встречается около 10k ± 5%).
- Алиас: граничные длины 2, 3, 10, 11; запрещённые символы; зарезервированные слова.
- URL: схемы `ftp` и `javascript`, пустой хост, 2049 символов, собственный домен, blocklist.
- `SingleFlight`: 100 одновременных `do()` → `fn` вызван 1 раз, все получили один результат; исключение доходит до всех ожидающих.
- `Guard`: при занятых слотах через 50 мс → `Overloaded`.
- TTL: джиттер в пределах ±10%; при `expires_at` через 1 час TTL ≤ 3600.

### Интеграционные (`tests/integration`, httpx против `localhost:8080`)

- Создание → редирект 302, `Location` совпадает с исходным URL.
- Свой алиас → 201, повтор с тем же алиасом → 409.
- `Idempotency-Key`: два последовательных POST → один и тот же `code`, в БД одна строка.
- `Idempotency-Key`: 20 параллельных POST с одним ключом → у всех один `code`, в `links` одна строка.
- Код с прошедшим `expires_at` → 410. Неизвестный код → 404.
- Чужая ссылка: GET info и DELETE → 403. Без токена → 401.
- DELETE → 204. Затем редирект → 410 на всех нодах не позже 10 с. Повторный DELETE → 404.
- Создание свободного алиаса, который до этого запрашивали (и получили 404) → сразу 302, без ожидания 60 с.

---

## 9. Сценарии отказов (ручная демонстрация, записать в README)

| Сценарий | Команда | Ожидаемо |
| --- | --- | --- |
| Падение ноды | `docker compose stop app2` | редиректы идут без ошибок (nginx повторяет GET); через 10 с нода исключена из ротации |
| Падение Redis primary | `docker compose stop redis-primary` | редиректы работают через L1 и БД, растёт латентность; через ~5 с Sentinel переключает на replica, ноды переподключаются |
| Redis полностью недоступен | остановить всё, что связано с Redis | редиректы работают из БД; single-flight сдерживает поток запросов; `redis_errors_total` растёт |
| Падение PG replica | `docker compose stop pg-replica` | чтение уходит на primary, ошибок нет |
| Падение PG primary | `docker compose stop pg-primary` | редиректы работают (кэш и реплика); POST → 503 + `Retry-After`; ручной promote: `docker compose exec -u postgres pg-replica pg_ctl promote -D /var/lib/postgresql/data` |
| Перегрузка | k6 на 3000 RPS | часть ответов 429/503 сразу, без лавины таймаутов; ноды не падают |

---

## 10. Нагрузочный тест

`load/seed.sql`: 100k ссылок. md5 в hex это подмножество base62, поэтому коды валидны.

```sql
INSERT INTO links (code, long_url, owner_id)
SELECT substr(md5(i::text), 1, 7), 'https://example.com/page/' || i, 1
FROM generate_series(1, 100000) AS i
ON CONFLICT DO NOTHING;
```

`load/redirect.js`:

```js
import http from 'k6/http';
import { check } from 'k6';

const codes = JSON.parse(open('/load/codes.json'));

export const options = {
  scenarios: {
    redirects: {
      executor: 'constant-arrival-rate',
      rate: 700, timeUnit: '1s', duration: '2m',
      preAllocatedVUs: 100, maxVUs: 400,
    },
  },
  thresholds: {
    http_req_duration: ['p(99)<50'],
    http_req_failed: ['rate<0.001'],
  },
};

export default function () {
  // перекос как в жизни: малая доля ссылок получает большую часть трафика
  const i = Math.floor(codes.length * Math.pow(Math.random(), 3));
  const r = http.get(`${__ENV.BASE}/${codes[i]}`, { redirects: 0 });
  check(r, { '302': (x) => x.status === 302 });
}
```

Порядок: `make up` → `make seed` → `make codes` → `make load`.

Что записать в отчёт:
- p50, p95 и p99 из k6;
- доли попаданий L1, L2 и БД из метрик;
- RPS, который дошёл до БД (ожидаем около 5% после прогрева).

На ноутбуке результат ориентировочный: всё работает на одной машине, и k6 конкурирует с сервисом за CPU.

---

## 11. Что отличается от продовой схемы

| Локально | Прод |
| --- | --- |
| nginx без TLS, один экземпляр | TLS на L7, 2+ экземпляра или облачный LB |
| PG primary и replica, ручной promote | Patroni (или managed PG) с автоматическим failover и VIP/DNS |
| миграция через `initdb.d` | alembic или dbmate отдельным job до деплоя |
| 1 Sentinel, кворум 1 | 3 Sentinel в разных зонах, кворум 2 |
| Redis 256 МБ | `maxmemory 6gb` на инстансе 8 ГБ |
| пользователь `shortener` для всего | отдельные роли: app (DML), migrator (DDL), replicator |
| секреты в `.env` | Vault или Kubernetes Secrets |
| 3 контейнера на одной машине | 3 ноды в 3 зонах, HPA по CPU/RPS |

---

## 12. Чек-лист перед сдачей

- [ ] `make reset && make up` с нуля поднимает всё без ручных действий.
- [ ] Unit и интеграционные тесты зелёные.
- [ ] k6: 700 RPS, p99 < 50 мс (или честно записано, почему на ноутбуке хуже).
- [ ] Все 6 сценариев отказов прогнаны, результаты в README.
- [ ] README: запуск, архитектура (диаграмма), ключевые решения (302 vs 301, random vs counter, idempotency в БД, cache-aside с инвалидацией, почему нет Kafka).
