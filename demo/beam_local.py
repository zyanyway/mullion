"""
python version = 2.7

data processing:  read data from local sqlite -> write the data to gbq
use Beam model to construct the data processing, set the computing enging to Direct, which runs locally.

"""

from __future__ import print_function

import apache_beam as beam
from google.cloud import bigquery
import pandas as pd
import sqlite3
import os
import logging
from apache_beam.options.pipeline_options import PipelineOptions


class ReadData(beam.DoFn):
    '''
    The model of reading data from sqlite
    '''

    def process(self, context):
        conn = sqlite3.connect(
            "./db/run_5b7252f014da39f6cabbc5f8_cad2e964-2e6c-4dfc-8818-e4bf7b42b816_results_dbs_global_merge_output_block.db")
        df = pd.read_sql_query(
            "select * from stock_reporting_results limit 10;", conn)
        conn.close()

        return df.values


class WriteData(beam.DoFn):
    '''
    The model of writing data to bigquery. The Beam emite data one by one
    '''

    def process(self, context):
        # the context is the one record of the records from the data

        logging.info('context is:', context, type(context))

        bigquery_client = bigquery.Client()
        dataset_ref = bigquery_client.dataset('my_dataset_id')

        table_ref = dataset_ref.table('my_table')
        table = bigquery_client.get_table(table_ref)

        content = [context]
        bigquery_client.insert_rows(table, content)

        return list('1')


class Test(beam.DoFn):

    def process(self, context):
        print(context)
        for i in context:
            print(i)


def bq_create_dataset():
    # create dataset if there is no the assigned dataset

    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('my_dataset_id')

    try:
        bigquery_client.get_dataset(dataset_ref)

    except Exception as e:
        print(e)
        dataset = bigquery.Dataset(dataset_ref)
        dataset = bigquery_client.create_dataset(dataset)
        print('Dataset {} created.'.format(dataset.dataset_id))


def bq_create_table():
    # create table is there is no the assigned table

    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset('my_dataset_id')

    # Prepares a reference to the table
    table_ref = dataset_ref.table('my_table')

    try:
        table = bigquery_client.get_table(table_ref)

    except Exception as e:
        schema = [
            bigquery.SchemaField(
                'stock_reporting_results_id_pk', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('localDomainId', 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('date_dimension_id_fk',
                                 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('location_dimension_id_fk',
                                 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('poolinfo_dimension_id_fk',
                                 'INTEGER', mode='NULLABLE'),
            bigquery.SchemaField('value', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('itemCount', 'INTEGER', mode='NULLABLE'),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = bigquery_client.create_table(table)
        print('table {} created.'.format(table.table_id))


def run():
    bq_create_dataset()
    bq_create_table()

    # construct the DAG, put it the pipline, run it locally
    with beam.Pipeline(options=PipelineOptions()) as p:
        temp = p | 'Initializing..' >> beam.Create(['1'])
        records = temp | 'Read records from sqlite' >> beam.ParDo(ReadData())
        # records | 'Print' >> beam.ParDo(Test())
        result = records | 'Write to BQ' >> beam.ParDo(WriteData())

    print("Done")


if __name__ == '__main__':
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './credential/demo2zy-b720f17009f0.json'
    run()
