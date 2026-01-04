"""
Star Wars ETL Pipeline
======================
Загружаем персонажей из SWAPI → MinIO → PostgreSQL
"""

from datetime import datetime
import json
import os
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator

# Настройки
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "youtube_data")
POSTGRES_USER = os.getenv("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "airflow")

# Зеркало SWAPI (оригинал имеет проблемы с SSL)
SWAPI_BASE_URL = "https://swapi.py4e.com/api"


def extract_people():
    """Извлекаем всех персонажей из SWAPI и сохраняем в MinIO"""
    from minio import Minio
    from io import BytesIO

    # Получаем данные из API
    all_people = []
    url = f"{SWAPI_BASE_URL}/people/"

    while url:
        print(f"Запрашиваем: {url}")
        response = requests.get(url)
        data = response.json()
        all_people.extend(data['results'])
        url = data.get('next')  # следующая страница

    print(f"Получено {len(all_people)} персонажей")

    # Сохраняем в MinIO
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    bucket_name = "starwars-raw"
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Сохраняем каждого персонажа отдельно
    for person in all_people:
        # Извлекаем ID из URL (например, из "https://swapi.dev/api/people/1/")
        swapi_id = person['url'].rstrip('/').split('/')[-1]

        json_data = json.dumps(person, ensure_ascii=False).encode('utf-8')
        file_path = f"people/{swapi_id}.json"

        client.put_object(
            bucket_name,
            file_path,
            BytesIO(json_data),
            len(json_data),
            content_type="application/json"
        )

    print(f"Сохранено в MinIO: {len(all_people)} файлов")
    return {"count": len(all_people)}


def transform_people():
    """Читаем из MinIO, трансформируем данные"""
    from minio import Minio

    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

    bucket_name = "starwars-raw"
    transformed = []

    # Получаем список всех файлов
    objects = client.list_objects(bucket_name, prefix="people/")

    for obj in objects:
        response = client.get_object(bucket_name, obj.object_name)
        person = json.loads(response.read().decode('utf-8'))
        response.close()

        # Извлекаем ID
        swapi_id = int(person['url'].rstrip('/').split('/')[-1])

        # Извлекаем homeworld_id
        homeworld_id = None
        if person.get('homeworld'):
            homeworld_id = int(person['homeworld'].rstrip('/').split('/')[-1])

        # Трансформируем
        transformed.append({
            "swapi_id": swapi_id,
            "name": person['name'],
            "height": person.get('height'),
            "mass": person.get('mass'),
            "hair_color": person.get('hair_color'),
            "skin_color": person.get('skin_color'),
            "eye_color": person.get('eye_color'),
            "birth_year": person.get('birth_year'),
            "gender": person.get('gender'),
            "homeworld_id": homeworld_id
        })

    print(f"Трансформировано: {len(transformed)} персонажей")
    return transformed


def load_people(**context):
    """Загружаем в PostgreSQL"""
    import psycopg2

    ti = context['ti']
    people = ti.xcom_pull(task_ids='transform_people')

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cur = conn.cursor()

    for person in people:
        cur.execute("""
            INSERT INTO people (swapi_id, name, height, mass, hair_color,
                               skin_color, eye_color, birth_year, gender, homeworld_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (swapi_id) DO UPDATE SET
                name = EXCLUDED.name,
                height = EXCLUDED.height,
                mass = EXCLUDED.mass,
                hair_color = EXCLUDED.hair_color,
                skin_color = EXCLUDED.skin_color,
                eye_color = EXCLUDED.eye_color,
                birth_year = EXCLUDED.birth_year,
                gender = EXCLUDED.gender,
                homeworld_id = EXCLUDED.homeworld_id,
                updated_at = CURRENT_TIMESTAMP
        """, (
            person['swapi_id'],
            person['name'],
            person['height'],
            person['mass'],
            person['hair_color'],
            person['skin_color'],
            person['eye_color'],
            person['birth_year'],
            person['gender'],
            person['homeworld_id']
        ))

    conn.commit()
    cur.close()
    conn.close()

    print(f"Загружено в PostgreSQL: {len(people)} персонажей")


# DAG
with DAG(
    dag_id="starwars_etl",
    start_date=datetime(2026, 1, 1),
    schedule=None,  # запускаем вручную
    catchup=False,
    description="ETL: SWAPI → MinIO → PostgreSQL",
    tags=["starwars", "etl"]
) as dag:

    task_extract = PythonOperator(
        task_id="extract_people",
        python_callable=extract_people,
    )

    task_transform = PythonOperator(
        task_id="transform_people",
        python_callable=transform_people,
    )

    task_load = PythonOperator(
        task_id="load_people",
        python_callable=load_people,
    )

    task_extract >> task_transform >> task_load
