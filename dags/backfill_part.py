import json
import pathlib
import pandas as pd
import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG,macros
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import sys
import os
crawling_path = os.path.join(os.path.dirname(os.path.realpath(__file__)),'../extract_data')
sys.path.append(crawling_path)
# sys.path.append('/home/dbsgh3322/estate_project/extract_data/')
from crawling_estates import CrawlingEstatesInfo
import datetime
import pendulum
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
import glob


gcs_bucket = 'estate_bucket'
project = 'estate-project-382208'
dataset = 'estate_dataset'
bigquery_table = 'for_sale'
bigquery_agg_table = 'for_sale_agg'
# dt_now = datetime.datetime.now().strftime('%Y%m%d')

time_z = pendulum.timezone('Asia/Seoul')
dag = DAG(
    dag_id= "backfill_part_of_estate",
    description= "crawling songdo apt info",
    start_date= datetime.datetime(2023,5,10,tzinfo= time_z),
    schedule_interval= "@daily",
    catchup=False,
    # schedule_interval= "@hourly",
    # schedule_interval=None
)


del_query_in_bq = BigQueryExecuteQueryOperator(
    task_id = "del_query_in_bq",
    gcp_conn_id = "bigquery_default",
    sql = [
        f"delete {project}.{dataset}.{bigquery_table} where " +
        "confirmYmd={{ ds_nodash }}"
    ],
    write_disposition = "WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag

)


gcsToBigQuery = GoogleCloudStorageToBigQueryOperator(
    task_id = 'gcs_to_bq', 
    gcp_conn_id = 'bigquery_default',
    destination_project_dataset_table = f'{dataset}.{bigquery_table}', 
    bucket = gcs_bucket, 
    source_objects = ["estate/songdo/*_{{ ds_nodash }}.csv"],
    source_format = 'CSV',
    write_disposition='WRITE_APPEND',
    # create_disposition = 'CREATE_IF_NEEDED',
    dag=dag
)


daily_for_sale_agg = BigQueryExecuteQueryOperator(
    task_id = "daily_for_sale_agg",
    gcp_conn_id = "bigquery_default",
    sql = [
        f"delete {project}.{dataset}.{bigquery_agg_table} where " + 
        "confirmYmd={{ ds_nodash }};",
        f"insert into {project}.{dataset}.{bigquery_agg_table} (confirmYmd,apt_name,count) \
        SELECT confirmYmd,apt_name,sum(1) as count \
        FROM {project}.{dataset}.{bigquery_table} " +
        "where confirmYmd={{ ds_nodash }} group by apt_name,confirmYmd;"],
    write_disposition = "WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag
    )

# gcsToBigQuery >> daily_for_sale_agg
# daily_for_sale_agg
del_query_in_bq >> gcsToBigQuery >> daily_for_sale_agg