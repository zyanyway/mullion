"""
python version: 2.7

data processing:  read data from local sqlite -> write the data to gbq
locally run the data processing using API of google cloud

"""

from __future__ import print_function

from google.cloud import bigquery
import pandas as pd
import sqlite3
import os
import logging


def read_data():
    '''
    using pandas to read the local sqlite
    '''
    conn = sqlite3.connect(
        "./db/run_5b7252f014da39f6cabbc5f8_cad2e964-2e6c-4dfc-8818-e4bf7b42b816_results_dbs_global_merge_output_block.db")
    df = pd.read_sql_query(
        "select * from stock_reporting_results limit 10;", conn)
    conn.close()

    return df


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


def write_bq(data):
    '''
    write data to bigquery, using the API of pandas. 
    The to_gbq can create the table automatically according Dataframe column labels info
    '''
    bq_create_dataset()
    data.to_gbq('my_dataset_id.my_table',
                   'demo2zy', if_exists='replace')


if __name__ == '__main__':

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './credential/demo2zy-b720f17009f0.json'

    write_bq(read_data())

