from airflow import DAG
from datetime import date, timedelta, datetime
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.dummy_operator import DummyOperator

import os
DAG_DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

BASE_DIR_PYTHON_SCRIPTS = '/mnt/c/Users/Matt/Desktop/Projet_perso/Airflow'
BASE_DIR = '/mnt/c/Users/Matt/Desktop/source'
BASE_DIR_TRANSFORMED = '/mnt/c/Users/Matt/Desktop/transformed'
FILES = ['dim_produits.csv','dim_clients.csv','dim_employes.csv','fact_orders.csv']
with DAG('dataware_v1', start_date=datetime(2019, 7, 22), schedule_interval='@daily', default_args=DAG_DEFAULT_ARGS, catchup=False) as dag:

    waiting_source_files_tasks = [FileSensor(task_id='waiting_source_files_{}'.format(
        f), fs_conn_id='fs_default', filepath=os.path.join(BASE_DIR, f), poke_interval=5) for f in os.listdir(BASE_DIR)]
    
    tamp_task = DummyOperator(task_id='dumm')
    tamp_task_2 = DummyOperator(task_id='dumm_2')

    creating_tables_task = BashOperator(task_id='creating_tables_task',
                                        bash_command='python3 /mnt/c/Users/Matt/Desktop/Projet_perso/Airflow/create_tables.py')

    transforming_tasks = [BashOperator(task_id='transforming_task_{}'.format(f),
                                       bash_command='python3 {}'.format(os.path.join(BASE_DIR_PYTHON_SCRIPTS, f))) for f in os.listdir(BASE_DIR_PYTHON_SCRIPTS) if 'transform' in f]

    waiting_transformed_files_tasks = [FileSensor(task_id='waiting_transformed_files_{}'.format(
        f), fs_conn_id='fs_default', filepath=os.path.join(BASE_DIR_TRANSFORMED, f), poke_interval=5) for f in FILES]

    loading_dim_taks = [BashOperator(task_id='loading_{}'.format(f),
                                     bash_command='python3 {}'.format(os.path.join(BASE_DIR_PYTHON_SCRIPTS, f))) for f in os.listdir(BASE_DIR_PYTHON_SCRIPTS) if f != 'load_fact_orders.py' and 'load' in f]
    loading_fact_task = BashOperator(task_id='loading_fact',
            bash_command='python3 {}'.format(os.path.join(BASE_DIR_PYTHON_SCRIPTS, 'load_fact_orders.py')))
    waiting_source_files_tasks >> tamp_task >> transforming_tasks >> tamp_task_2 >> waiting_transformed_files_tasks >> creating_tables_task >> loading_dim_taks >> loading_fact_task

