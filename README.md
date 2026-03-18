## Запуск

```powershell
docker compose up -d --build
```

## Генерация данных

```powershell
docker compose run --rm worker python -m replicator.generate_data
```

## Ручной запуск репликации

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

## Полный сброс

```powershell
docker compose down -v
```
