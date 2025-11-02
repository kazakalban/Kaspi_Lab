from airflow.sdk import dag, task
import time
from pathlib import Path
import json

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


# 2 Practice
@dag(
    dag_id="etl_local_to_local_dag",
    description="practice lesson 2",
    schedule="0 5 * * 1,3,5"
)
def etl_local_to_local_dag():
    
    @task
    def extract_raw_data():
        # Пути к файлам
        raw_path = Path("/Users/kasym/Kaspi_Lab/2_modul_Airflow/row_production.json")
        staging_path = Path("/Users/kasym/Kaspi_Lab/2_modul_Airflow/staging_products.json")
    
        # Проверка существования входного файла
        if not raw_path.exists():
            raise FileNotFoundError(f"Файл не найден {raw_path}")
        
        # Чтение JSON
        try:
            with open(raw_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            print(f"Успешно прочитан {raw_path}")
        except json.JSONDecodeError as e:
            raise ValueError(f"Ошибка при разборе JSON: {e}")
        
        
    
    
    
    
    @task
    def transform_data():
        pass

    @task
    def load_data():
        pass
