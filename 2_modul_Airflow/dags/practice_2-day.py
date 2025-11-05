from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import os
import json

JSON_URL: str = "https://jsonplaceholder.typicode.com/comments"

default_args={"owner":"Kassym[DE]",
                    "retries":1,
                    "retry_delay":timedelta(minutes=2)}

@dag(
    dag_id='comments_json_to_postgres',
    description="Creat DAG when run ETL",
    schedule=timedelta(days=1),
    start_date=datetime(2025,11,3),
    default_args=default_args,
    tags=["download_json",
            "wait_for_file",
            "create_table",
            "parse_json",
            "load_to_postgres"], # В интерфейсе Airflow можно будет быстро найти DAG по этим тегам
    catchup=False # нужно ли догонять пропущенные запуски False
    )

def etl_create_comments_table():
    """ETL процесс: JSON → Postgres"""

    @task
    def download_json():
        data_path: str = "/opt/airflow/data/comments.json"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        response = requests.get(JSON_URL)

        with open(data_path, "w") as file:
            file.write(response.text)
        print("This task download_json")


    @task.sensor(poke_interval=10, mode="reschedule")
    def wait_for_file():
        file_path: str = "/opt/airflow/data/comments.json"
        """Проверяет, существует ли файл"""
        file_exists = os.path.exists(file_path)
        if file_exists:
            print(f"Файл найден: {file_path}")
        else:
            print(f"Файла ещё нет: {file_path}")
        return file_exists 


    @task
    def create_table():
        """Создает таблицу, если нет"""
        postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS comments ( 
                id INTEGER PRIMARY KEY,
                post_id INTEGER,
                name TEXT,
                email TEXT,
                body TEXT
            );
        """)
        conn.commit()
        print("This task create_table")


    @task(multiple_outputs = True)
    def parse_json():
        data_path: str = "/opt/airflow/data/comments.json"
        """Парсит JSON"""
        with open(data_path, "r") as file:
            data = json.load(file)
        print("This task parse_json")
        return {"count":len(data),
                "sourse": JSON_URL, 
                "data":data}

    @task(run_if=lambda context: context["ti"].xcom_pull(task_ids="parse_json")["count"] > 0)    
    @task()
    def load_to_postgres(count: int, data: list):
        """Загружает данные в Postgres"""

        #if count <= 0:
         #   print("Нет данных для загрузки — пропуск задачи")
          #  return
        postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        inserted = 0
        for row in data:
            cursor.execute("""
                INSERT INTO comments (id, post_id, name, email, body)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;    
            """, (row["id"], row["postId"], row["name"], row["email"], row["body"]))
            inserted += cursor.rowcount # просто если хоти узнать сколько добавил
        conn.commit()
        print("This task load_to_postgres")


    # === Вызовы ===
    download_task = download_json()
    wait_task = wait_for_file()
    create_task = create_table()
    parsed_task = parse_json()
    load_task = load_to_postgres(count=parsed_task["count"], data=parsed_task["data"])

    download_task >> wait_task >> create_task >> parsed_task >> load_task


etl_create_comments_table()
