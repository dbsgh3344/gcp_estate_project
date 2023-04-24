import json
import pathlib

import airflow.utils.dates
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import sys
sys.path.append('/home/dbsgh3322/estate_project/extract_data/')
from crawling_estates import CrawlingEstatesInfo
import datetime
import pendulum
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
import glob
import os

# log_path = '/home/dbsgh3322/testlog.txt'
# with open(log_path,'w') as f:
#     f.write('success dag')

time_z = pendulum.timezone('Asia/Seoul')
dag = DAG(
    dag_id= "crawling_songdo_apt",
    description= "crawling songdo apt info",
    start_date= datetime.datetime(2023,4,19,tzinfo= time_z),
    schedule_interval= "@daily",
    # schedule_interval= "@hourly",
)
# start_date= datetime.datetime.now().strftime('%Y%m%d')
c= CrawlingEstatesInfo('new.land.naver.com')
get_regions= PythonOperator(
    task_id='get_regions',
    python_callable= c.get_specific_region,
    op_kwargs= {'cityname':'인천시','gu':'연수구','dong':'송도동'},
    dag= dag)

upload_file_list = []
def _upload_file():
    gcs = GCSHook()
    bk = 'estate_bucket'
    cur_path = os.path.dirname(os.path.realpath(__file__))
    tmp_path = os.path.join(cur_path,'testdata')
    filelist = glob.glob(os.path.join(tmp_path,'*'))
    log_path = os.path.join(cur_path,'logs','file_upload.txt')
    daily_dir = datetime.datetime.now().strftime('%Y%m%d')

    for f in filelist:
        object_filepath = f'estate/songdo/{daily_dir}/{os.path.basename(f)}'
        gcs.upload(
            bucket_name = bk,
            object_name = object_filepath,
            filename = f
        )
        msg = f'success upload {f}'
        with open(log_path,'a') as fs :
            fs.write(msg+'\n')

        os.remove(f)
        # upload_file_list.append(object_filepath)

            

upload_files = PythonOperator(
    task_id = 'upload_file',
    python_callable = _upload_file,
    dag = dag
)

bk = 'estate_bucket'
# p = 'estate/songdo/20230406_송도동_e편한세상송도.csv'
gcsToBigQuery = GoogleCloudStorageToBigQueryOperator(
    task_id = 'gcs_to_bq', 
    gcp_conn_id = 'bigquery_default',
    destination_project_dataset_table = 'tmp.test2', 
    bucket = bk, 
    source_objects = [f"estate/songdo/{datetime.datetime.now().strftime('%Y%m%d')}/*.csv"],
    source_format = 'CSV',
    write_disposition='WRITE_APPEND',
    create_disposition = 'CREATE_IF_NEEDED',
    dag=dag
)


get_regions >> upload_files >> gcsToBigQuery
# get_regions