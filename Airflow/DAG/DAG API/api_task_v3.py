from datetime import timedelta
from airflow import DAG # type: ignore
import requests
from airflow.utils.dates import days_ago # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.hooks.postgres_hook import PostgresHook # type: ignore
from airflow.providers.postgres.operators.postgres import PostgresOperator # type: ignore
from airflow.operators.dummy import DummyOperator # type: ignore
from airflow.utils.task_group import TaskGroup  # type: ignore
from psycopg2 import extras # type: ignore


default_args = {
    "owner": "volov",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

# Конфигурация DAG
with DAG(
    "volov",
    default_args=default_args,
    description="Auto-generated DAG for Rick & Morty API data ingestion",
    schedule_interval="@daily",
    concurrency=1,
    max_active_runs=1,
    catchup=False,
    start_date=days_ago(2),
    tags=["Rick&Morty API"],
) as dag:

    # Dummy-операторы — маркируем старт и конец DAG для визуализации,
    start = DummyOperator(task_id="start")
    end = DummyOperator(task_id="end")

    # Универсальная функция для загрузки данных из любого переданного API.
    def fetch_and_save(
        table_name,
        url,
        fields_sql,
        fields_list,
        postgres_conn_id,
        **kwargs
    ):
        """
        Загружает все страницы данных из публичного API,
        сохраняет очищенные данные в таблицу Postgres пакетно.
        table_name    -- название SQL-таблицы
        url           -- стартовый url API
        fields_sql    -- строка для SQL-запроса (id, name, ...)
        fields_list   -- список ключей из json (['id', 'name', ...])
        postgres_conn_id -- имя подключения Airflow
        """
        pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        data_to_insert = []

        print(f"Начало загрузки данных для таблицы {table_name}")

        while url:
            response = requests.get(url)
            if response.status_code != 200:
                raise ValueError(f"Ошибка API: {response.status_code}")
            data = response.json()
            results = data.get("results", [])
            for item in results:
                row = tuple(item.get(field) for field in fields_list)
                data_to_insert.append(row)
            url = data.get('info', {}).get('next')

        insert_query = f"""INSERT INTO {table_name} ({fields_sql})
                           VALUES %s
                           ON CONFLICT (id) DO NOTHING;"""

        if data_to_insert:
            extras.execute_values(cursor, insert_query, data_to_insert)
            print(f"Вставлено строк в таблицу {table_name}: {len(data_to_insert)}")
        
        conn.commit()
        cursor.close()
        conn.close()
        print(f"Загрузка данных для таблицы {table_name} завершена")
        return f"Данные для {table_name} успешно загружены"
    

    # Конфигурация для всех таблиц и API — одно место для масштабирования!
    TABLES = [
        {
            'name': 'chars',
            'url': 'https://rickandmortyapi.com/api/character',
            'fields_sql': 'id, name, status, species, type, gender',
            'fields_list': ['id', 'name', 'status', 'species', 'type', 'gender'],
            'ddl': """
                CREATE TABLE IF NOT EXISTS chars (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    status TEXT,
                    species TEXT,
                    type TEXT,
                    gender TEXT
                );
            """,
        },
        {
            'name': 'locs',
            'url': 'https://rickandmortyapi.com/api/location',
            'fields_sql': 'id, name, type, dimension',
            'fields_list': ['id', 'name', 'type', 'dimension'],
            'ddl': """
                CREATE TABLE IF NOT EXISTS locs (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    type TEXT,
                    dimension TEXT
                );
            """,
        },
        {
            'name': 'eps',
            'url': 'https://rickandmortyapi.com/api/episode',
            'fields_sql': 'id, name, air_date, episode',
            'fields_list': ['id', 'name', 'air_date', 'episode'],
            'ddl': """
                CREATE TABLE IF NOT EXISTS eps (
                    id INTEGER PRIMARY KEY,
                    name TEXT,
                    air_date TEXT,
                    episode TEXT
                );
            """,
        },
    ]

    # Генерация пайплайна для каждой таблицы через TaskGroup
    for table in TABLES:
        group_id = f"{table['name']}_group"
        with TaskGroup(group_id=group_id) as tg:
            # 1. Создание таблицы
            create_table = PostgresOperator(
                task_id=f"create_table_{table['name']}",
                postgres_conn_id="volov_pg",
                sql=table['ddl'],
            )

            # 2. Очистка таблицы TRUNCATE
            truncate_table = PostgresOperator(
                task_id=f"truncate_table_{table['name']}",
                postgres_conn_id="volov_pg",
                sql=f"TRUNCATE TABLE {table['name']};",
            )

            # 3. Загрузка данных из API c bulk insert
            save_data = PythonOperator(
                task_id=f"save_data_{table['name']}",
                python_callable=fetch_and_save,
                op_kwargs={
                    "table_name": table['name'],
                    "url": table['url'],
                    "fields_sql": table['fields_sql'],
                    "fields_list": table['fields_list'],
                    "postgres_conn_id": "volov_pg"
                },
            )
            # Очерёдность
            create_table >> truncate_table >> save_data


        start >> tg >> end