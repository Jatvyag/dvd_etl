import operator
import time
import psycopg2
from datetime import datetime, timedelta
from functools import reduce

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.docker.operators.docker import DockerOperator


def wait_for_postgres(**kwargs):
    """ Test connection for database. """
    attempt = 0
    while attempt < 20:
        try:
            pg_hook = PostgresHook(postgres_conn_id='postgres')
            pg_hook.get_conn()
            print('PostgreSQL is ready for connections')
            return
        except psycopg2.OperationalError:
            print('Waiting for PostgreSQL to be ready...')
            time.sleep(5)
            attempt += 1
    raise Exception('PostgreSQL is not ready after waiting')

def dump_table_to_csv(**kwargs):
    """ Save flat files of database tables (csv format). """
    pg_hook = PostgresHook(postgres_conn_id='postgres')
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
    concurrency=10,
    tags=['ingesting'],
) as dag:
    container_name='postgres_cont'
    
    # Create DB and open connection for next steps.
    start_postgres = DockerOperator(
        task_id='start_postgres',
        image='postgres:latest',
        container_name=container_name,
        auto_remove=True,
        xcom_all=True,
        environment={
            'POSTGRES_DB': '{{ var.value.db_postgres }}',
            'POSTGRES_USER': '{{ var.value.db_user }}', # The user name should be 'postgres', because we download it from website.
            'POSTGRES_PASSWORD': '{{ var.value.db_pass }}',
            },
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge',
        port_bindings={'5432':'5431'}, # We need to configure other port for this DB, because it might be in use on local machine.
    )
    
    # Check if connection is established successfully.
    wait_postgres = PythonOperator(
        task_id='wait_postgres',
        python_callable=wait_for_postgres,
    )
    
    # Download dvd DB schema.
    download_schema = BashOperator(
        task_id='download_schema',
        depends_on_past=False,
        bash_command='wget https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-schema.sql -P {{ var.value.dump_path }}', # Absolute path to 'dumps' directory,
    )

    # Create necessary tables.
    create_schema = PostgresOperator(
        task_id='create_schema',
        depends_on_past=False,
        postgres_conn_id='postgres',
        autocommit=True,
        sql='dumps/pagila-schema.sql',
    )

    # Download data.
    download_data = BashOperator(
        task_id='download_data',
        depends_on_past=False,
        bash_command='wget https://raw.githubusercontent.com/devrimgunduz/pagila/master/pagila-insert-data.sql -P {{ var.value.dump_path }}',
    )
    
    # Populate data into DB inside Docker.
    populate_data = PostgresOperator(
        task_id='populate_data',
        depends_on_past=False,
        postgres_conn_id='postgres',
        sql='dumps/pagila-insert-data.sql',
    )

    # Download table names for flat files names.
    list_tables = PostgresOperator(
        task_id='list_tables',
        depends_on_past=False,
        postgres_conn_id='postgres',
        sql="SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE'",
        do_xcom_push=True,
    )

    # Create flat files.
    create_csv = PythonOperator(
        task_id='create_csv',
        depends_on_past=False,
        python_callable=dump_table_to_csv,
    )

    # Finishe the DAG with container stop.
    stop_postgres = BashOperator(
        task_id='stop_postgres',
        depends_on_past=True,
        bash_command=f'docker stop {container_name}',
    )
    
    start_postgres
    wait_postgres >> download_schema >> create_schema >> download_data >> populate_data >> list_tables >> create_csv >> stop_postgres