from datetime import timedelta, date
from airflow.utils.dates import days_ago
from airflow.decorators import dag, task

# Аргументы
default_args = {
    "owner": "vj-volov",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="vj-volov_3", # Имя DAG
    default_args=default_args, # Аргументы по умолчанию
    description="DAG, version 1", # Описание DAG
    schedule_interval="@daily", # Периодичность запуска
    start_date=days_ago(2), # Дата начала
    tags=["theory"], # Теги
)

def task_flow():
    @task()
    def get_date():
        return str(date.today())
    date_str = get_date()
    print(f"Today is {date_str}")
task_flow()