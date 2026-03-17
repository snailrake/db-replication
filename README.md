# db-replication

0. создаю `.env` в корне проекта

Проще всего так:

```powershell
Copy-Item .env.example .env
```

В `.env` лежат:
- настройки PostgreSQL
- настройки MongoDB
- настройки worker
- cron
- параметры генерации данных

Если контейнеры работают внутри `docker-compose`, то вместо `localhost` используются имена сервисов:
- PostgreSQL = `postgres`
- MongoDB = `mongo`

1. `Dockerfile` в корне проекта

```dockerfile
FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY pyproject.toml .
COPY src ./src

RUN pip install --no-cache-dir .

CMD ["python", "-m", "replicator.main"]
```

`FROM` - образ на основе `python:3.11-slim`

`WORKDIR` - рабочая директория внутри контейнера

`ENV` - переменные для python

`COPY pyproject.toml .` - копируется файл с зависимостями

`COPY src ./src` - копируется код проекта

`RUN pip install --no-cache-dir .` - ставятся зависимости

`CMD ["python", "-m", "replicator.main"]` - запускается worker

2. `docker-compose.yml` в корне проекта

В проекте поднимаются 3 контейнера:
- `postgres`
- `mongo`
- `worker`

`postgres` - основная реляционная база

`mongo` - база, куда складывается реплика

`worker` - скрипт, который читает данные из PostgreSQL, преобразует их и записывает в MongoDB

Также подключены volume:
- `postgres_data`
- `mongo_data`

Из-за этого данные сохраняются и не пропадают после остановки контейнеров.

3. запуск проекта

```powershell
docker compose up -d --build
```

Если нужно остановить:

```powershell
docker compose down
```

Если нужно удалить контейнеры и volume и поднять заново:

```powershell
docker compose down -v
docker compose up -d --build
```

4. как создается структура базы

При первом запуске PostgreSQL выполняет SQL-файлы из папки `db/init`:
- `01_schema.sql`
- `02_seed.sql`

Создаются таблицы:
- `customers`
- `orders`
- `products`
- `order_products`

Связи:
- `customers -> orders` - один-ко-многим
- `orders -> products` - многие-ко-многим

Также добавлены:
- индексы
- поле `deleted_at`
- триггеры для `updated_at`

Во втором файле лежат стартовые тестовые данные.

5. если нужно сгенерировать много данных

Для генерации есть отдельный скрипт:

```powershell
docker compose run --rm worker python -m replicator.generate_data
```

По умолчанию в `.env.example` стоят:
- `SEED_CUSTOMERS=100000`
- `SEED_PRODUCTS=500`
- `SEED_ORDERS=500000`

Если нужно, можно поменять их в `.env`.

6. запуск репликации

Если нужно вручную выполнить один цикл:

```powershell
docker compose run --rm -e WORKER_MODE=once worker
```

Логика такая:
1. worker читает время последней синхронизации
2. ищет измененные данные в PostgreSQL
3. собирает данные по затронутым клиентам
4. преобразует их в MongoDB-документы
5. записывает их в MongoDB
6. сохраняет новое время синхронизации

В MongoDB один клиент хранится как один документ, внутри которого:
- массив заказов
- внутри заказа массив товаров

Если нужен запуск по интервалу, в `.env` можно поставить:

```env
WORKER_MODE=interval
SYNC_INTERVAL_SECONDS=60
```

Если нужен запуск по cron:

```env
WORKER_MODE=cron
SYNC_CRON=*/2 * * * *
```

7. проверка результата

Для проверки есть команда:

```powershell
docker compose run --rm worker python -m replicator.main verify
```

Она показывает:
- количество записей в PostgreSQL
- количество документов в MongoDB
- состояние синхронизации
- пример одного документа

8. дополнительные задания

- добавил таблицу `products`
- сделал связь `orders -> products`
- вынес настройки в `.env`
- добавил `deleted_at`
- сделал защиту от дублей при повторном запуске

Пример soft delete:

```sql
UPDATE orders
SET deleted_at = NOW()
WHERE id = 1;
```

После следующего цикла репликации это изменение попадет в MongoDB.

9. доп инфа

Docker Compose сам создает общую сеть по умолчанию, поэтому отдельно сеть здесь создавать не обязательно.

Контейнеры внутри compose обращаются друг к другу по именам сервисов:
- `postgres`
- `mongo`
- `worker`

Volume в `docker-compose` нужны для того, чтобы данные PostgreSQL и MongoDB сохранялись между перезапусками.

`depends_on` нужен для того, чтобы worker ждал запуска баз.

`restart: unless-stopped` у worker нужен для автоматического перезапуска контейнера.

10. полезные команды

Запуск:

```powershell
docker compose up -d --build
```

Остановка:

```powershell
docker compose down
```

Полный сброс:

```powershell
docker compose down -v
```

Ручной запуск репликации:

```powershell
docker compose run --rm -e WORKER_MODE=once worker
```

Генерация данных:

```powershell
docker compose run --rm worker python -m replicator.generate_data
```

Проверка:

```powershell
docker compose run --rm worker python -m replicator.main verify
```
