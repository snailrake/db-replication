## Что делает проект

Проект делает фоновую репликацию данных из PostgreSQL в MongoDB.

PostgreSQL здесь основная база, MongoDB реплика.
Worker читает изменения из PostgreSQL, преобразует данные и записывает их в MongoDB.

## Что где лежит

- `db/init` - SQL для создания таблиц и стартовых данных
- `src/replicator/main.py` - основной запуск
- `src/replicator/replication.py` - логика репликации
- `src/replicator/generate_data.py` - генерация большого количества данных
- `docker-compose.yml` - запуск контейнеров
- `Dockerfile` - контейнер для worker

## Запуск

```powershell
docker compose up -d --build
```

## Генерация данных

```powershell
docker compose run --rm worker python -m replicator.generate_data
```

## Если нужно вручную запустить репликацию

```powershell
docker compose run --rm -e WORKER_MODE=once worker
```

## Проверка

```powershell
docker compose run --rm worker python -m replicator.main verify
```

## Остановка

```powershell
docker compose down
```

## Если нужно полностью сбросить

```powershell
docker compose down -v
```

## Дополнительные задания

- добавлена таблица `products`
- сделана связь `orders -> products`
- настройки вынесены в `.env`
- добавлен `deleted_at`
- сделана защита от дублей при повторном запуске
