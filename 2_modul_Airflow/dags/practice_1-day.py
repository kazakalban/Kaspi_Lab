# 1 и 2 задачи 1 лекции 

from airflow.sdk import dag, task
import time
from datetime import timedelta
from pathlib import Path
import json
from typing import TypedDict


# 1 Practice
@dag(
    dag_id="simple_pipeline_dag",
    description="practice lesson 1",
    schedule="0/60 * * * *"
)
def simple_pipeline_dag():
    @task
    def start() -> None:
        """1. start - выводит в лог: 'Pipeline started'"""
        print("Pipeline started")

    @task
    def fetch_raw_data() -> None:
        """2. fetch_raw_data — имитирует получение данных (time.sleep(2)
            и печатает 'Raw data fetched')."""
        time.sleep(10)
        print("Raw data fetched")
    
    @task
    def process_data() -> None:
        """3. process_data — печатает 'Data processed'."""
        print("Data processed")

    @task
    def finish() -> None:
        """4. finish — печатает 'Pipeline finished'."""
        print("Pipeline finished")

    # Make tasks dependent on each other
    start() >> fetch_raw_data() >> process_data() >> finish()
    
simple_pipeline_dag()


# типизация
class Product(TypedDict, total=False):
    id: int | str
    name: str
    price: int | float | str
    with_vat: float

# 2 Practice
@dag(
    dag_id="etl_local_to_local_dag",
    description="practice lesson 2",
    schedule="0 5 * * 1,3,5"
)
def etl_local_to_local_dag():
    
    raw_path: Path = Path("/opt/airflow/data/raw_products.json") # Основная 
    staging_path: Path = Path("/opt/airflow/data/staging_products.json") # Чистая
    clean_path: Path = Path("/opt/airflow/data/products_clean.json") # Готовая


    @task(retries = 2, retry_delay=timedelta(seconds=60)) # Попыток 2 с задержкой 60 секунд
    def extract_raw_data():
        # Провераю существования входного файла
        if not raw_path.exists():
            raise FileNotFoundError(f"Файл не найден {raw_path}")
        
        # Читаю JSON
        try:
            with open(raw_path, "r", encoding="utf-8") as f:
                data: list[Product] = json.load(f)
            print(f"Успешно прочитан {raw_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Ошибка при разборе JSON: {e}")
        
        # Записьоваю в string-файл
        with open(staging_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        print(f"Данные записаны в {staging_path}")
    
    @task(retries = 2, retry_delay=timedelta(seconds=60)) # Попыток 2 с задержкой 60 секунд
    def transform_data():
        # Провераю существования входного файла
        if not staging_path.exists():
            raise FileNotFoundError(f"Файл не найден {staging_path}")
    
        # Читаю JSON
        with open(staging_path, "r", encoding="utf-8") as f:
            products: list[Product]  = json.load(f)

        transformed: list[Product] = [] # сюда буду складовать преобразованные записи

        # Обработваю каждую запись
        for p in products:
            try:
                # Преобразую строку в цело число
                price_int: int = int(float(p.get("price", 0)))

                # Добавляю поле с НДС 12%
                with_vat: float = round(price_int * 1.12, 2)

                # Обновляю запись
                p["price"] = price_int
                p["with_vat"] = with_vat

                # Добавляю итоговый список
                transformed.append(p)
            except Exception as e:
                print(f"Ошика при обработке записи {p}: {e}")

        with open(clean_path, "w", encoding="utf-8") as f:
            json.dump(transformed, f, ensure_ascii=False, indent=4)

        print(f"Трансформация завершена. Молодец!")
    
    @task
    # **context нужен, чтобы получить служебную информацию от 
    # Airflow о текущем запуске задачи
    def load_data(**context):
        # Провераю существования входного файла
        if not clean_path.exists():
            raise FileNotFoundError(f"Файл не найден {clean_path}")
        
        #Читаю JSON
        with open(clean_path, "r", encoding="utf-8") as f:
            products: list[Product] = json.load(f)
        
        # получаю количество строк
        count: int = len(products)

        # airflow автоматом передает даты в execute_date
        execution_date: str = context["ds"] # ds = дата запуска в формате YYYY-MM-DD

        # Логирование
        print("========== LOAD DATA REPORT ==========")
        print(f"Обработано записей: {count}")
        print(f"Дата запуска DAG: {execution_date}")
        print("======================================")

    row = extract_raw_data()
    transformed = transform_data()
    loaded = load_data()

    # порядок выполнения
    row >> transformed >> loaded

etl_local_to_local_dag()