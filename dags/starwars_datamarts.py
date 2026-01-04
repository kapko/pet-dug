"""
Star Wars Data Marts
====================
Создание и обновление витрин данных
"""

from datetime import datetime
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "youtube_data")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")


def get_connection():
    """Получить подключение к PostgreSQL"""
    import psycopg2
    return psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


def create_tables():
    """Создать таблицы витрин если не существуют"""
    conn = get_connection()
    cur = conn.cursor()

    # Витрина: статистика по полу
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dm_gender_stats (
            gender VARCHAR(20) PRIMARY KEY,
            total_count INTEGER,
            avg_height NUMERIC(10,2),
            avg_mass NUMERIC(10,2),
            min_height NUMERIC(10,2),
            max_height NUMERIC(10,2),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Витрина: топ персонажей
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dm_top_characters (
            category VARCHAR(50),
            rank INTEGER,
            name VARCHAR(255),
            value NUMERIC(10,2),
            gender VARCHAR(20),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (category, rank)
        )
    """)

    # Витрина: цвет глаз
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dm_eye_color_stats (
            eye_color VARCHAR(50) PRIMARY KEY,
            total_count INTEGER,
            percentage NUMERIC(5,2),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("Таблицы витрин созданы")


def build_gender_stats():
    """Витрина: статистика по полу"""
    conn = get_connection()
    cur = conn.cursor()

    # Очищаем и заполняем заново
    cur.execute("TRUNCATE TABLE dm_gender_stats")

    cur.execute("""
        INSERT INTO dm_gender_stats (gender, total_count, avg_height, avg_mass, min_height, max_height, updated_at)
        SELECT
            gender,
            COUNT(*) as total_count,
            ROUND(AVG(NULLIF(height, 'unknown')::numeric), 2) as avg_height,
            ROUND(AVG(NULLIF(REPLACE(mass, ',', ''), 'unknown')::numeric), 2) as avg_mass,
            MIN(NULLIF(height, 'unknown')::numeric) as min_height,
            MAX(NULLIF(height, 'unknown')::numeric) as max_height,
            CURRENT_TIMESTAMP
        FROM people
        WHERE gender IS NOT NULL
        GROUP BY gender
        ORDER BY total_count DESC
    """)

    conn.commit()

    # Выводим результат
    cur.execute("SELECT * FROM dm_gender_stats")
    rows = cur.fetchall()
    print("dm_gender_stats:")
    for row in rows:
        print(f"  {row}")

    cur.close()
    conn.close()


def build_top_characters():
    """Витрина: топ персонажей по характеристикам"""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE dm_top_characters")

    # Топ-5 самых высоких
    cur.execute("""
        INSERT INTO dm_top_characters (category, rank, name, value, gender, updated_at)
        SELECT
            'tallest' as category,
            ROW_NUMBER() OVER (ORDER BY NULLIF(height, 'unknown')::numeric DESC) as rank,
            name,
            NULLIF(height, 'unknown')::numeric as value,
            gender,
            CURRENT_TIMESTAMP
        FROM people
        WHERE height != 'unknown' AND height IS NOT NULL
        LIMIT 5
    """)

    # Топ-5 самых тяжёлых
    cur.execute("""
        INSERT INTO dm_top_characters (category, rank, name, value, gender, updated_at)
        SELECT
            'heaviest' as category,
            ROW_NUMBER() OVER (ORDER BY NULLIF(REPLACE(mass, ',', ''), 'unknown')::numeric DESC) as rank,
            name,
            NULLIF(REPLACE(mass, ',', ''), 'unknown')::numeric as value,
            gender,
            CURRENT_TIMESTAMP
        FROM people
        WHERE mass != 'unknown' AND mass IS NOT NULL
        LIMIT 5
    """)

    # Топ-5 самых низких
    cur.execute("""
        INSERT INTO dm_top_characters (category, rank, name, value, gender, updated_at)
        SELECT
            'shortest' as category,
            ROW_NUMBER() OVER (ORDER BY NULLIF(height, 'unknown')::numeric ASC) as rank,
            name,
            NULLIF(height, 'unknown')::numeric as value,
            gender,
            CURRENT_TIMESTAMP
        FROM people
        WHERE height != 'unknown' AND height IS NOT NULL
        LIMIT 5
    """)

    conn.commit()

    # Выводим результат
    cur.execute("SELECT * FROM dm_top_characters ORDER BY category, rank")
    rows = cur.fetchall()
    print("dm_top_characters:")
    for row in rows:
        print(f"  {row}")

    cur.close()
    conn.close()


def build_eye_color_stats():
    """Витрина: распределение по цвету глаз"""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE dm_eye_color_stats")

    cur.execute("""
        INSERT INTO dm_eye_color_stats (eye_color, total_count, percentage, updated_at)
        SELECT
            eye_color,
            COUNT(*) as total_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as percentage,
            CURRENT_TIMESTAMP
        FROM people
        WHERE eye_color IS NOT NULL AND eye_color != 'unknown'
        GROUP BY eye_color
        ORDER BY total_count DESC
    """)

    conn.commit()

    # Выводим результат
    cur.execute("SELECT * FROM dm_eye_color_stats ORDER BY total_count DESC")
    rows = cur.fetchall()
    print("dm_eye_color_stats:")
    for row in rows:
        print(f"  {row}")

    cur.close()
    conn.close()


# DAG
with DAG(
    dag_id="starwars_datamarts",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # Запускаем после основного ETL
    catchup=False,
    description="Построение витрин данных Star Wars",
    tags=["starwars", "datamart"]
) as dag:

    task_create_tables = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )

    task_gender_stats = PythonOperator(
        task_id="build_gender_stats",
        python_callable=build_gender_stats,
    )

    task_top_characters = PythonOperator(
        task_id="build_top_characters",
        python_callable=build_top_characters,
    )

    task_eye_color = PythonOperator(
        task_id="build_eye_color_stats",
        python_callable=build_eye_color_stats,
    )

    # Сначала создаём таблицы, потом параллельно заполняем витрины
    task_create_tables >> [task_gender_stats, task_top_characters, task_eye_color]
