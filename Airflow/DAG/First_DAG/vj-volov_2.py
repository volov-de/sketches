from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Аргументы
default_args = {
    "owner": "vj-volov",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Использование конструкции `with`
with DAG(
    dag_id="vj-volov_2", # Имя DAG
    default_args=default_args, # Аргументы по умолчанию
    description="DAG, version 2", # Описание DAG
    schedule_interval="@daily", # Периодичность запуска
    start_date=days_ago(2), # Дата начала
    tags=["theory"], # Теги
) as dag:
    # Таск используя BashOperator
    print_date = BashOperator(
        task_id="print_date",
        bash_command="date"
    )
    
    print_date