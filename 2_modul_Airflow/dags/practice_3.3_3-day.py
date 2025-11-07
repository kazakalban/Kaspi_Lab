from datetime import datetime, timedelta
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import os
import json
from collections import defaultdict, Counter
from statistics import mean


# Константы
JSON_URLS = {
    "comments": "https://jsonplaceholder.typicode.com/comments",
    "posts": "https://jsonplaceholder.typicode.com/posts"
}

DATA_DIR = "/opt/airflow/data"

default_args = {
    "owner": "Kassym[DE]",
    "retries": 1,
    "retry_delay": timedelta(minutes=2)
}


def get_cursor():
    """Возвращает соединение и курсор PostgreSQL через Airflow Connection 'pg_conn'."""
    hook = PostgresHook(postgres_conn_id="pg_conn")
    conn = hook.get_conn()
    cursor = conn.cursor()
    return conn, cursor


@dag(
    dag_id="etl_create_comments_and_posts_table",
    description="ETL: загрузка JSON → Postgres",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 11, 3),
    default_args=default_args,
    catchup=False,
    tags=["etl", "json", "postgres", "aggregation"],
)
def etl_create_comments_and_posts_table():
    """Основной ETL-процесс: скачивает JSON, сохраняет в Postgres и агрегирует данные."""

    @task
    def download_json():
        """Скачивает JSON файлы posts и comments в локальную директорию."""
        os.makedirs(DATA_DIR, exist_ok=True)
        for name, url in JSON_URLS.items():
            response = requests.get(url)
            response.raise_for_status()
            file_path = os.path.join(DATA_DIR, f"{name}.json")
            with open(file_path, "w") as f:
                f.write(response.text)
            print(f"Downloaded {name} -> {file_path}")
        return True

    @task.sensor(poke_interval=10, mode="reschedule")
    def wait_for_file():
        """Ждёт, пока оба JSON файла появятся и будут не пустыми."""
        files = [
            os.path.join(DATA_DIR, "comments.json"),
            os.path.join(DATA_DIR, "posts.json"),
        ]

        for file_path in files:
            if not os.path.exists(file_path):
                print(f"Файл не найден: {file_path}")
                return False
            if os.path.getsize(file_path) == 0:
                print(f"Пустой файл: {file_path}")
                return False
            print(f"Файл готов: {file_path}")
        return True

    @task
    def create_stage_tables():
        """Создает промежуточные (staging) таблицы в Postgres."""
        conn, cursor = get_cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stg_comments (
                id INTEGER PRIMARY KEY,
                post_id INTEGER,
                name TEXT,
                email TEXT,
                body TEXT
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stg_posts (
                id INTEGER PRIMARY KEY,
                title TEXT,
                body TEXT
            );
        """)

        conn.commit()
        cursor.close()
        conn.close()
        print("Stage tables created")

    @task(multiple_outputs=True)
    def parse_json():
        """Парсит скачанные JSON файлы и возвращает словарь с данными и количеством записей."""
        result = {}
        for name in ["comments", "posts"]:
            file_path = os.path.join(DATA_DIR, f"{name}.json")
            with open(file_path, "r") as file:
                data = json.load(file)
                result[name] = {"count": len(data), "data": data}
        print("Parsed JSON files")
        return result

    @task
    def load_stg_to_postgres(result_parse_json: dict):
        """Загружает данные из JSON в промежуточные таблицы."""
        conn, cursor = get_cursor()
        inserted = 0

        for key, value in result_parse_json.items():
            data = value["data"]
            if key == "comments":
                for row in data:
                    cursor.execute("""
                        INSERT INTO stg_comments (id, post_id, name, email, body)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;
                    """, (row["id"], row["postId"], row["name"], row["email"], row["body"]))
                    inserted += cursor.rowcount
            elif key == "posts":
                for row in data:
                    cursor.execute("""
                        INSERT INTO stg_posts (id, title, body)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (id) DO NOTHING;
                    """, (row["id"], row["title"], row["body"]))
                    inserted += cursor.rowcount

        conn.commit()
        cursor.close()
        conn.close()
        print(f"Inserted {inserted} rows into staging tables")

    @task
    def create_clean_tables():
        """Создает аналитические таблицы (fact, dq, users)."""
        conn, cursor = get_cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS fact_post_comments (
                id INTEGER PRIMARY KEY,
                post_id INTEGER,
                post_title TEXT,
                comments_count INTEGER,
                avg_comment_len INTEGER
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS dq_missed_post_comments (
                id INTEGER PRIMARY KEY,
                comment_id INTEGER,
                post_id INTEGER,
                email TEXT,
                body TEXT
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                created_posts_cnt INTEGER
            );
        """)

        conn.commit()
        cursor.close()
        conn.close()
        print("Clean tables created")

    @task
    def aggregate_all(result_parse_json: dict):
        """Объединяет данные posts и comments и формирует агрегаты."""
        posts = result_parse_json["posts"]["data"]
        comments = result_parse_json["comments"]["data"]

        comments_by_post = defaultdict(list)
        for c in comments:
            comments_by_post[c["postId"]].append(c)

        fact_post_comments = []
        for post in posts:
            post_id = post["id"]
            post_comments = comments_by_post.get(post_id, [])
            comments_count = len(post_comments)
            avg_comment_len = mean(len(c["body"]) for c in post_comments) if post_comments else 0
            fact_post_comments.append({
                "id": post_id,
                "post_id": post_id,
                "post_title": post["title"],
                "comments_count": comments_count,
                "avg_comment_len": round(avg_comment_len, 2)
            })

        existing_post_ids = {p["id"] for p in posts}
        dq_missed_post_comments = [
            {
                "id": i + 1,
                "comment_id": c["id"],
                "post_id": c["postId"],
                "email": c["email"],
                "body": c["body"],
            }
            for i, c in enumerate(comments)
            if c["postId"] not in existing_post_ids
        ]

        user_counts = Counter([p["userId"] for p in posts])
        users = [{"user_id": uid, "created_posts_cnt": cnt} for uid, cnt in user_counts.items()]

        print("Aggregation complete")
        return {
            "fact_post_comments": fact_post_comments,
            "dq_missed_post_comments": dq_missed_post_comments,
            "users": users,
        }

    @task
    def load_fact_to_postgres(aggregated: dict):
        """Загружает агрегированные данные в таблицы fact, dq и users."""
        conn, cursor = get_cursor()

        for row in aggregated["fact_post_comments"]:
            cursor.execute("""
                INSERT INTO fact_post_comments
                (id, post_id, post_title, comments_count, avg_comment_len)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                SET comments_count = EXCLUDED.comments_count,
                    avg_comment_len = EXCLUDED.avg_comment_len;
            """, (
                row["id"], row["post_id"], row["post_title"],
                row["comments_count"], row["avg_comment_len"]
            ))

        for row in aggregated["dq_missed_post_comments"]:
            cursor.execute("""
                INSERT INTO dq_missed_post_comments
                (id, comment_id, post_id, email, body)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """, (
                row["id"], row["comment_id"], row["post_id"],
                row["email"], row["body"]
            ))

        for row in aggregated["users"]:
            cursor.execute("""
                INSERT INTO users (user_id, created_posts_cnt)
                VALUES (%s, %s)
                ON CONFLICT (user_id) DO UPDATE
                SET created_posts_cnt = EXCLUDED.created_posts_cnt;
            """, (row["user_id"], row["created_posts_cnt"]))

        conn.commit()
        cursor.close()
        conn.close()
        print("Aggregated data loaded into Postgres")

    # Последовательность задач
    download = download_json()
    wait = wait_for_file()
    create_stage = create_stage_tables()
    parse = parse_json()
    load_stage = load_stg_to_postgres(parse)
    create_clean = create_clean_tables()
    aggregate = aggregate_all(parse)
    load_fact = load_fact_to_postgres(aggregate)

    download >> wait >> create_stage >> parse >> load_stage >> create_clean >> aggregate >> load_fact


etl_create_comments_and_posts_table()
