from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
import requests
from airflow.hooks.postgres_hook import PostgresHook

# URL Rick & Morty API
URL = "https://rickandmortyapi.com/api/character"

def fetch_and_save_characters():
    """
    Получает персонажей из Rick & Morty API и сохраняет их в таблицу chars.
    """
    pg_hook = PostgresHook(postgres_conn_id="volov_pg")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    # Очищаем таблицу перед загрузкой новых данных
    cursor.execute("TRUNCATE TABLE chars;")

    next_url = URL
    while next_url:
        response = requests.get(next_url)
        if response.status_code != 200:
            raise ValueError(f"Ошибка API: {response.status_code}")
        data = response.json()
        results = data.get('results', [])
        for char in results:
            insert_query = """
                INSERT INTO chars (id, name, status, species, type, gender)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING;
            """
            cursor.execute(insert_query, (
                char['id'],
                char['name'],
                char['status'],
                char['species'],
                char['type'],
                char['gender']
            ))
        # Переход к следующей странице API
        next_url = data.get('info', {}).get('next')
    conn.commit()
    cursor.close()
    conn.close()

# Аргументы по умолчанию для DAG
default_args = {
    "owner": "volov",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Описание DAG
with DAG(
    dag_id="volov",
    default_args=default_args,
    description="DAG для загрузки персонажей Rick & Morty",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["Rick&Morty API"],
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1, 
) as dag:

    # Оператор для создания таблицы chars, если она не существует
    create_table = PostgresOperator(
        task_id="create_chars_table",
        postgres_conn_id="volov_pg",
        sql="""
        CREATE TABLE IF NOT EXISTS chars (
            id INTEGER PRIMARY KEY,
            name TEXT,
            status TEXT,
            species TEXT,
            type TEXT,
            gender TEXT
        );
        """
    )

    # Оператор для загрузки и сохранения персонажей
    fetch_and_save = PythonOperator(
        task_id="fetch_and_save_characters",
        python_callable=fetch_and_save_characters,
        provide_context=True
    )

    # Задаём порядок выполнения: сначала создание таблицы, затем загрузка данных
    create_table >> fetch_and_save
