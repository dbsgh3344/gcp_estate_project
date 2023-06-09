import json
import pathlib
import pandas as pd
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG,macros
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models.variable import Variable
import os
import sys
# cur_path = os.path.dirname(os.path.realpath(__file__))
# sys.path.append('/home/dbsgh3322/estate_project/extract_data/')
crawling_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),'../extract_data')
sys.path.append(crawling_path)
from crawling_estates import CrawlingEstatesInfo
import datetime
import pendulum
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.sftp.operators.sftp import SFTPOperator
import glob

gcs_bucket = 'estate_bucket'
project = 'estate-project-382208'
dataset = 'estate_dataset'
bigquery_table = 'for_sale'
bigquery_agg_table = 'for_sale_agg'
# dt_now = datetime.datetime.now().strftime('%Y%m%d')

time_z = pendulum.timezone('Asia/Seoul')
dag = DAG(
    dag_id= "celery_test",
    description= "test",
    start_date= datetime.datetime(2023,6,7,tzinfo= time_z),
    # schedule_interval= "@daily",
    # schedule_interval= "@hourly",
    # schedule_interval=None,
    schedule_interval = "0 23 * * *",
    catchup=False
)


def _worker_test(ds_nodash,**context) :
    cur_path =os.path.dirname(os.path.realpath(__file__)) +"/test.txt"
    with open(cur_path,'w') as f:
        f.write('tmp test'+" {{ds_nodash}}")


test_task = PythonOperator(
    task_id= 'test_task',
    python_callable=_worker_test,
    provide_context=True,
    dag=dag
)

test_task