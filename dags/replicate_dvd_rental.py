from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime, timedelta
from functools import reduce
import operator


def dump_table_to_csv(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_local')
    ti = kwargs['ti']
    list_lists_tables = ti.xcom_pull(task_ids='list_tables')
    list_tables = reduce(operator.concat, list_lists_tables)
    for name in list_tables:
        if name != 'payment':
            output_file = f'source/{name}.csv'
            pg_hook.bulk_dump(table=name, tmp_file=output_file)

default_args = {
'owner': '{{ var.value.owner }}',
'email': '{{ var.value.email }}',
'start_date': datetime(2024, 7, 16),
'depends_on_past': False,
'email_on_failure': False,
'email_on_retry': False,
'retries': 3,
'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='replicate_dvd_rental',
    default_args=default_args,
    description='Pipeline to replicate DVD rental DB and make csv files',
    start_date=datetime(2024, 7, 16),
    schedule_interval='@once',
    catchup=False,
    tags=['ingesting'],
) as dag:
    
    download_schema = BashOperator(
        task_id='download_schema',
        depends_on_past=False,
        bash_command='wget https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-schema.sql -P {{ var.value.dump_path }}',
    )

    create_schema = PostgresOperator(
        task_id='create_schema',
        depends_on_past=False,
        postgres_conn_id='postgres_local',
        sql='dumps/pagila-schema.sql',
    )

    download_data = BashOperator(
        task_id='download_data',
        depends_on_past=False,
        bash_command='wget https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-insert-data.sql -P {{ var.value.dump_path }}',
    )

    populate_data = PostgresOperator(
        task_id='populate_data',
        depends_on_past=False,
        postgres_conn_id='postgres_local',
        sql='dumps/pagila-insert-data.sql',
    )

    list_tables = PostgresOperator(
        task_id='list_tables',
        depends_on_past=False,
        postgres_conn_id='postgres_local',
        sql="SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'",
        do_xcom_push=True,
    )

    create_csv = PythonOperator(
        task_id='create_csv',
        depends_on_past=False,
        python_callable=dump_table_to_csv,
    )

    download_schema >> create_schema >> download_data >> populate_data >> list_tables >> create_csv