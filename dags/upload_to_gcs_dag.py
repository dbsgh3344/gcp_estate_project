from airflow.providers.google.cloud.hooks.gcs import GCSHook
import glob
import os
# import json
# import pathlib
import airflow.utils.dates
# import requests
# import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from crawling_estates import CrawlingEstatesInfo
import datetime
import pendulum
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator



time_z = pendulum.timezone('Asia/Seoul')
dag = DAG(
    dag_id= "gcp_test",
    description= "only test gcp",
    start_date= datetime.datetime(2023,4,18,15,40,tzinfo= time_z),
    schedule_interval= "@once",
    # schedule_interval= "@hourly",
)

def _upload_file():
    gcs= GCSHook()
    bk = 'estate_bucket'
    tmp_path ='/home/dbsgh3322/airflow/dags/testdata/'
    filelist=glob.glob(os.path.join(tmp_path,'*'))    
    cur_path = os.path.dirname(os.path.realpath(__file__))
    log_path = os.path.join(cur_path,'logs','file_upload.txt')

    for f in filelist:
        gcs.upload(
            bucket_name=bk,
            object_name=f'estate/songdo/{os.path.basename(f)}',
            filename=f
        )

        msg = f'success upload {f}'
        with open(log_path,'a') as fs :
            fs.write(msg+'\n')
        os.remove(f)

upload_files = PythonOperator(
    task_id = 'upload_file',
    python_callable = _upload_file,
    dag = dag
)

# def _load_from_gcs_to_bq() :
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/dbsgh3322/estate-project-382208-d1789fa4560c.json"
bk = 'estate_bucket'
p = 'estate/songdo/20230406_송도동_e편한세상송도.csv'
gcsToBigQuery = GoogleCloudStorageToBigQueryOperator(
    task_id = 'gcs_to_bq', 
    gcp_conn_id = 'bigquery_default',
    destination_project_dataset_table = 'tmp.test2', 
    bucket = bk, 
    source_objects = [p],
    source_format = 'CSV',
    write_disposition='WRITE_APPEND',
    create_disposition = 'CREATE_IF_NEEDED',
    dag=dag
)


# upload_files
# gcsToBigQuery