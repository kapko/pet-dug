# Star Wars Data Pipeline

ETL-пайплайн для загрузки данных из Star Wars API в Data Warehouse.

## Архитектура

```
[SWAPI API] → [Airflow] → [MinIO] → [PostgreSQL]
                            ↓
                      (Data Lake)      (DWH)
```

## Технологии

- **Airflow** — оркестрация ETL
- **MinIO** — S3-совместимое хранилище (Data Lake)
- **PostgreSQL** — хранилище данных (DWH)
- **Docker** — контейнеризация

## Быстрый старт

### 1. Клонируй репозиторий

```bash
git clone <repo-url>
cd pet-dug
```

### 2. Создай `.env` файл

```bash
cp .env.example .env
```

### 3. Запусти контейнеры

```bash
docker-compose up
```

### 4. Открой сервисы

| Сервис | URL | Логин | Пароль |
|--------|-----|-------|--------|
| **Airflow** | http://localhost:8080 | `admin` | `admin` |
| **MinIO** | http://localhost:9001 | `minioadmin` | `minio_secret_2026` |
| **PostgreSQL** | localhost:5432 | `airflow` | `postgres_secret_2026` |

## DAGs

### `starwars_etl`
Основной ETL-пайплайн:
- **extract_people** — загрузка персонажей из SWAPI
- **transform_people** — трансформация данных
- **load_people** — загрузка в PostgreSQL

### `starwars_datamarts`
Построение витрин данных:
- **dm_gender_stats** — статистика по полу
- **dm_top_characters** — топ персонажей
- **dm_eye_color_stats** — распределение по цвету глаз

## Структура базы данных

### Таблицы (DWH)

```sql
people          -- персонажи Star Wars
planets         -- планеты
raw_swapi_data  -- сырые данные
```

### Витрины (Data Marts)

```sql
dm_gender_stats     -- статистика по полу
dm_top_characters   -- топ персонажей
dm_eye_color_stats  -- распределение по цвету глаз
```

## Примеры запросов

```sql
-- Все персонажи
SELECT name, height, mass, gender FROM people;

-- Статистика по полу
SELECT * FROM dm_gender_stats;

-- Топ-5 самых высоких
SELECT * FROM dm_top_characters
WHERE category = 'tallest'
ORDER BY rank;
```

## Структура проекта

```
pet-dug/
├── dags/
│   ├── starwars_etl.py        # ETL пайплайн
│   └── starwars_datamarts.py  # Витрины данных
├── sql/
│   └── init.sql               # Схема БД
├── logs/                      # Логи Airflow
├── docker-compose.yml
├── .env                       # Секреты (не в git)
├── .env.example               # Шаблон секретов
└── README.md
```

## Остановка

```bash
docker-compose down      # остановить
docker-compose down -v   # остановить + удалить данные
```