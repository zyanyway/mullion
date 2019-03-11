"""
python version = 2.7

data processing:  read data from local sqlite -> write the data to gbq
use Beam model to construct the data processing, set the computing enging to Dataflow, which runs on the Google Dataflow server.

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

    def process(self, context):

        conn = sqlite3.connect(
            "./db/run_5b7252f014da39f6cabbc5f8_cad2e964-2e6c-4dfc-8818-e4bf7b42b816_results_dbs_global_merge_output_block.db")
        df = pd.read_sql_query(
            "select * from stock_reporting_results limit 10;", conn)
        conn.close()

        return df.values


def read_data():
    conn = sqlite3.connect(
        "./db/run_5b7252f014da39f6cabbc5f8_cad2e964-2e6c-4dfc-8818-e4bf7b42b816_results_dbs_global_merge_output_block.db")
    df = pd.read_sql_query(
        "select * from stock_reporting_results limit 10;", conn)
    conn.close()

    return df.values


class WriteData(beam.DoFn):

    def process(self, context, data):

        logging.info('context is:', context, type(context))

        bigquery_client = bigquery.Client()
        dataset_ref = bigquery_client.dataset('my_dataset_id')

        table_ref = dataset_ref.table('my_table')
        table = bigquery_client.get_table(table_ref)

        bigquery_client.insert_rows(table, data)

        return list('1')


class Test(beam.DoFn):

    def process(self, context):
        print(context)
        for a in context:
            print(a)


def bq_create_dataset():
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
    data = read_data() # locally read data since cannot read local data from googld dataflow server

    # set the computing engine argumnets as googld dataflow
    pipline_args = ['--project', 'demo2zy',
                    '--job_name', 'myjob1',
                    '--staging_location', 'gs://my_demo/beam',
                    '--temp_location', 'gs://my_demo/temp',
                    '--runner', 'DataflowRunner',
                    '--save_main_session', 'True']


    with beam.Pipeline(argv=pipline_args) as p:

        temp = p | 'Initializing..' >> beam.Create(['1'])
        # records = temp | 'Read records from sqlite' >> beam.ParDo(ReadData())
        result = temp | 'Write to BQ' >> beam.ParDo(WriteData(), data)

    print("Done")


if __name__ == '__main__':

    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = './credential/demo2zy-b720f17009f0.json'
    run()