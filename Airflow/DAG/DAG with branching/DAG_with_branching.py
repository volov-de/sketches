from datetime import timedelta
from airflow import DAG  # type: ignore
from airflow.utils.dates import days_ago  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore
from airflow.operators.python import BranchPythonOperator  # type: ignore

default_args = {
    "owner": "vj-volov",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

##Функции
def is_weekday(**kwargs):
    from datetime import datetime
    execution_date_str = kwargs['ds']
    execution_date = datetime.strptime(execution_date_str, "%Y-%m-%d")
    if execution_date.weekday() < 5:  # 0-4: Пн-Пт
        return "send_xecution_date_in_xcom"
    else:
        return print("Выходной день. Задача пропущена.")

def send_execution_date(**kwargs):
    execution_date_str = kwargs['ds'] # Отправляем execution_date в xcom
    return execution_date_str

with DAG(
    "vj-volov_7",
    default_args=default_args,
    description="DAG with branching",
    schedule_interval="@daily",
    concurrency=1,
    max_active_runs=1,
    catchup=True,  # чтобы DAG отработал минимум за 7 дней
    start_date=days_ago(7),
    tags=["Branching"],
) as dag:

    check_day = BranchPythonOperator(
        task_id="check_day",
        python_callable=is_weekday, #Выполнение функции для проверки дня недели
    )

    send_execution_date_in_xcom = PythonOperator(
        task_id="send_execution_date_in_xcom",
        python_callable=send_execution_date, # Отправка execution_date в xcom
    )

    check_day >> send_execution_date_in_xcom
