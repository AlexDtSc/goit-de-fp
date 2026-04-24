# 4_project_solution.py

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Базові аргументи DAG
default_args = {
    'owner': 'alexdtsc',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Вказуємо жорсткий шлях всередині Docker-контейнера
DAGS_FOLDER = '/opt/airflow/dags'

with DAG(
    dag_id='alexdtsc_final_part2_datalake',                       # мій унікальний DAG ID
    default_args=default_args,
    description='Landing -> Bronze -> Silver -> Gold Spark ETL',
    schedule_interval=None,                                       # ручний запуск
    start_date=datetime(2026, 4, 24),                             # актуальна дата старту
    catchup=False,
    tags=["alexdtsc", "final_project"]
) as dag:

    # Завдання 1: Landing -> Bronze
    landing_to_bronze = SparkSubmitOperator(
        task_id='task_landing_to_bronze',
        application=f'{DAGS_FOLDER}/1_landing_to_bronze.py',
        conn_id='spark-default',
        verbose=1
    )

    # Звдання 2: Bronze -> Silver
    bronze_to_silver = SparkSubmitOperator(
        task_id='task_bronze_to_silver',
        application=f'{DAGS_FOLDER}/2_bronze_to_silver.py',
        conn_id='spark-default',
        verbose=1
    )

    # Звдання 3: Silver -> Gold
    silver_to_gold = SparkSubmitOperator(
        task_id='task_silver_to_gold',
        application=f'{DAGS_FOLDER}/3_silver_to_gold.py',
        conn_id='spark-default',
        verbose=1
    )
    
    # Порядок виконання (Конвеєр) - Встановлення послідовності
    landing_to_bronze >> bronze_to_silver >> silver_to_gold