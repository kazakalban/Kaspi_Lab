from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import os
import json

JSON_URLS = {
    "comments": "https://jsonplaceholder.typicode.com/comments",
    "posts": "https://jsonplaceholder.typicode.com/posts"
}


default_args={"owner":"Kassym[DE]",
                    "retries":1,
                    "retry_delay":timedelta(minutes=2)}

@dag(
    dag_id='etl_create_comments_and_posts_table',
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

def etl_create_comments_and_posts_table():
    """ETL процесс: JSON → Postgres"""

    # словарь с URL для скачивания
    JSON_URLS = {
        "comments": "https://jsonplaceholder.typicode.com/comments",
        "posts": "https://jsonplaceholder.typicode.com/posts",
    }

    DATA_DIR = "/opt/airflow/data"


    @task
    def download_json():
        """Скачивает два JSON файла"""
        os.makedirs(DATA_DIR, exist_ok=True)

        for name, url in JSON_URLS.items():
            response = requests.get(url)
            response.raise_for_status()
            file_path = os.path.join(DATA_DIR, f"{name}.json")
            with open(file_path, "w") as f:
                f.write(response.text)
            print(f"Downloaded {name} -> {file_path}")

        print("This task download_json")


    @task.sensor(poke_interval=10, mode="reschedule")
    def wait_for_file():
        """Ждёт, пока оба файла появятся"""
        files = [
            os.path.join(DATA_DIR, "comments.json"),
            os.path.join(DATA_DIR, "posts.json"),
        ]

        all_exist = True
        for file_path in files:
            if os.path.exists(file_path):
                if os.path.getsize(file_path) < 0: #  сразу проверью на пустоту
                    print(f"Файл пустой: {file_path}")
                    return False
                print(f"Файл найден: {file_path}")
            else:
                print(f"Файла ещё нет: {file_path}")
                all_exist = False

        return all_exist


    @task
    def create_table():
        """Создает таблицу, если нет"""
        postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stg_comments ( 
                id INTEGER PRIMARY KEY,
                post_id INTEGER,
                name TEXT,
                email TEXT,
                body TEXT
            );
            CREATE TABLE IF NOT EXISTS stg_posts (
                id INTEGER PRIMARY KEY,
                title TEXT,
                body TEXT
            );
        """)
        conn.commit()
        print("This task create_table")


    @task(multiple_outputs = True)
    def parse_json():
        """Парсит JSON файлы comments.json и posts.json"""
        files = {
            "comments": os.path.join(DATA_DIR, "comments.json"),
            "posts": os.path.join(DATA_DIR, "posts.json"),
        }

        result_parse_json = {}
        
        for key, file_path in files.items():
            with open(file_path, "r") as file:
                data = json.load(file)
                result_parse_json[key] = {
                    "count": len(data),
                    "data": data
                }
        print("This task parse_json")
        return result_parse_json


    #@task(run_if=lambda context: context["ti"].xcom_pull(task_ids="parse_json")["count"] > 0)    
    @task()
    def load_to_postgres(result_parse_json: dict):
        """Загружает данные в Postgres"""
        #print(result_parse_json)
        postgres_hook = PostgresHook(postgres_conn_id="pg_conn")
        conn = postgres_hook.get_conn()
        cursor = conn.cursor()

        inserted = 0
        for key,  value in result_parse_json.items():
            data = value["data"]
            if key == "comments":
                for row in data:
                    cursor.execute("""
                        INSERT INTO stg_comments (id, 
                                              post_id,
                                              name, 
                                              email, 
                                              body)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;    
                    """, (row["id"],
                          row["postId"], 
                          row["name"], 
                          row["email"], 
                          row["body"])
                    )
                    inserted += cursor.rowcount
                
            elif key == "posts":
                for row in data:
                    cursor.execute("""
                    INSERT INTO stg_posts (id,
                                       title,
                                       body)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (id) DO NOTHING;
                    """,
                    (row["id"], row["title"], row["body"])
                    )
                    inserted += cursor.rowcount # просто если хоти узнать сколько добавил
        conn.commit()
        print("This task load_to_postgres")



    # === Вызовы ===
    download_task = download_json()
    wait_task = wait_for_file()
    create_task = create_table()
    parsed_task = parse_json()
    load_task = load_to_postgres(parsed_task)

    download_task >> wait_task >> create_task >> parsed_task >> load_task

etl_create_comments_and_posts_table()
