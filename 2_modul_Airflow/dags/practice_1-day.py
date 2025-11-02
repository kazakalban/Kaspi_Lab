from airflow.sdk import dag, task
import time
@dag(
    dag_id="simple_pipeline_dag",
    description="practice lesson 1",
    schedule=None
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