from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from extract import extract_task
from verify import verify_main_data
from transform import process_data
from load_data import insert_data_to_sqlite

# Airflow variables
# Username:  airflow
# Password:  airflow

# Postgres connection
# Username:  airflow
# Password:  airflow
# dbname:    airflow_db
# port:      5432
# table:     fuel_sales
# columns:   year_month, uf, product, unit, volume, created_at

ORIGINAL_FILE = 'vendas-combustiveis-m3.xlsx'
MAIN_CACHE_FILE = 'main_cache.csv'
DIESEL_CACHE_FILE = 'diesel_cache.csv'
REFERENCE_FILE = 'reference.csv'
OUTPUT_FILE = 'fuel_sales.csv'
DB_FILE = 'fuel_sales.db'
DB_URL = 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow_db'

dag = DAG(
    'raizen_etl',
    description='Extrair informações de tabelas pivotadas.',
    schedule_interval='@daily',
    start_date=datetime(2024, 11, 7),
    catchup=False
)

extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_task,
    provide_context=True,
    op_args=[ORIGINAL_FILE, MAIN_CACHE_FILE, DIESEL_CACHE_FILE, REFERENCE_FILE],
    dag=dag
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=process_data,
    provide_context=True,
    op_args=[MAIN_CACHE_FILE, DIESEL_CACHE_FILE, OUTPUT_FILE],
    dag=dag
)

verify = PythonOperator(
    task_id='verify_data',
    python_callable=verify_main_data,
    provide_context=True,
    op_args=[OUTPUT_FILE, REFERENCE_FILE],
    dag=dag
)

loading = PythonOperator(
    task_id='load_data',
    python_callable=insert_data_to_sqlite,
    provide_context=True,
    op_args=[OUTPUT_FILE, DB_URL],
    dag=dag
)


extract >> transform >> verify >> loading