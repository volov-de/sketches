from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import requests

URL = "https://rickandmortyapi.com/api/character"

def fetch_character_count(**kwargs):
    response = requests.get(URL)
    
    if response.status_code != 200:
        raise ValueError(f"Ошибка API: {response.status_code}")
    
    data = response.json()
    count = data.get('info', {}).get('count')
    if count is not None:
        print(f"Count: {count}")
        return count
    else:
        raise ValueError("Не удалось получить количество персонажей из ответа API")

# Создание DAG
default_args = {
    "owner": "vj-volov",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="DAG_API_Rick&Morty",
    default_args=default_args,
    description="DAG для API Rick&Morty",
    schedule_interval="@daily",
    start_date=days_ago(1),
    concurrency=1, # Ограничение на количество одновременных задач
    catchup=False, # Не выполнять пропущенные запуски
    tags=["API Rick&Morty"],
) as dag:


    get_count_task = PythonOperator(
        task_id="get_count_task",
        python_callable=fetch_character_count,
        provide_context=True,  
    )

    get_count_task