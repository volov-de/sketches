from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Аргументы
default_args = {
    "owner": "vj-volov",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Описание DAG
dag = DAG(
    dag_id="vj-volov_1", # Имя DAG
    default_args=default_args, # Аргументы по умолчанию
    description="DAG, version 1", # Описание DAG
    schedule_interval="@daily", # Периодичность запуска
    start_date=days_ago(2), # Дата начала
    tags=["theory"], # Теги
)
# Функция
def print_hello_func():
    return "Hello, World!"

print_hello = PythonOperator(
    task_id="print_hello", 
    python_callable=print_hello_func,
    dag=dag
)

print_hello
