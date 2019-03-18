# from __future__ import print_function

from google.cloud import bigquery
import pandas as pd
import sqlite3
import os
import logging

import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime


def read_data(**context):
    '''
    using pandas to read the local sqlite
    '''
    conn = sqlite3.connect(
        "/usr/local/airflow/db/run_5b7252f014da39f6cabbc5f8_cad2e964-2e6c-4dfc-8818-e4bf7b42b816_results_dbs_global_merge_output_block.db")
    df = pd.read_sql_query(
        "select * from stock_reporting_results limit 10;", conn)
    conn.close()

    return df


def write_bq(**context):
    '''
    write data to bigquery, using the API of pandas.
    The to_gbq can create the table automatically according Dataframe column labels info
    '''
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/usr/local/airflow/credential/demo2zy-b720f17009f0.json'
    bq_create_dataset()
    ti = context['ti']
    data = ti.xcom_pull(task_ids='read_data')
    data.to_gbq('my_dataset_id.my_table',
                'demo2zy', if_exists='append')


def print_test(**context):
    ti = context['ti']
    data = ti.xcom_pull(task_ids='read_data')
    print(data)


def bq_create_dataset():
    '''
    if there is no dataset with assigned id in the google bigquery, create it
    '''
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('my_dataset_id')

    try:
        bigquery_client.get_dataset(dataset_ref)

    except Exception as e:
        # print(e)
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bigquery_client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))


args = {
    'owner': 'airflow',
    'start_date': datetime.now(),
    'provide_context': True,
}

dag = DAG(
    dag_id='example_1',
    default_args=args,
    schedule_interval="@once")

task1 = PythonOperator(
    task_id='read_data',
    python_callable=read_data,
    dag=dag,
)

task2 = PythonOperator(
    task_id='write_bq',
    python_callable=write_bq,
    dag=dag,
)

# read -> write
task1 >> task2
