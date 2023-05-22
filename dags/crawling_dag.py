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
sys.path.append('/home/dbsgh3322/estate_project/extract_data/')
from crawling_estates import CrawlingEstatesInfo
import datetime
import pendulum
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
import glob
import os

gcs_bucket = 'estate_bucket'
project = 'estate-project-382208'
dataset = 'estate_dataset'
bigquery_table = 'for_sale'
bigquery_agg_table = 'for_sale_agg'
# dt_now = datetime.datetime.now().strftime('%Y%m%d')

time_z = pendulum.timezone('Asia/Seoul')
dag = DAG(
    dag_id= "crawling_songdo_apt",
    description= "crawling songdo apt info",
    start_date= datetime.datetime(2023,5,10,tzinfo= time_z),
    # schedule_interval= "@daily",
    # schedule_interval= "@hourly",
    schedule_interval=None
)

def _crawling_task(ds_nodash,**kwargs) :
    c = CrawlingEstatesInfo('new.land.naver.com',ds_nodash)
    c.get_specific_region('인천시','연수구','송도동')



def _merge_daily_file(ds_nodash,**kwargs) :
    col_type = {
            'construction_company':'object',
            'use_approve':'int64',
            'detail_addrs':'string',
            'addrs':'object',
            'apt_code':'int64',
            'no':'int64',
            'apt_name':'object',
            'estate_name':'object',
            'trade_name':'object',
            'supply_area':'int64',
            'exclusive_area':'int64',
            'direction':'object',
            'confirmYmd':'int64',
            'latitude':'float64',
            'longitude':'float64',
            'price':'int64',
            'total_floor':'int64',
            'current_floor':'string'
            }
                
    # local_path = '/home/song/code/python_dir/tmpdir'
    cur_path = os.path.dirname(os.path.realpath(__file__))
    tmp_path = os.path.join(cur_path,'testdata')
    filelist = glob.glob(tmp_path+'/*.csv')
    # datelist = set(map(lambda x: os.path.basename(x).split('_')[0],filelist))
    # for d in datelist :
    #     globals()[f'dt_{d}']=pd.DataFrame()

    tmp = pd.DataFrame()
    for f in filelist :
        dt = os.path.basename(f).split('_')[0]
        if dt == ds_nodash:
            df = pd.read_csv(f)
            # globals()[f'dt_{dt}'] = pd.concat([globals()[f'dt_{dt}'],df])
            tmp = pd.concat([tmp,df])
            os.remove(f)
            
        
    # for d in datelist :
    # tmp = pd.DataFrame(globals()[f'dt_{d}'])
    tmp.drop_duplicates(subset=['no'],keep='last',inplace=True)
    tmp.dropna(inplace=True)
    tmp = tmp.astype(col_type)
    filename = os.path.join(tmp_path,f'songdo_{ds_nodash}.csv')
    tmp.to_csv(filename,index=False)



def _upload_file():
    gcs = GCSHook()
    # bk = 'estate_bucket'
    cur_path = os.path.dirname(os.path.realpath(__file__))
    tmp_path = os.path.join(cur_path,'testdata')
    filelist = glob.glob(os.path.join(tmp_path,'*'))
    log_path = os.path.join(cur_path,'logs','file_upload.txt')
    # daily_dir = datetime.datetime.now().strftime('%Y%m%d')

    for f in filelist:
        file_dt = os.path.basename(f).split('_')[0]
        object_filepath = f'estate/songdo/{os.path.basename(f)}'
        gcs.upload(
            bucket_name = gcs_bucket,
            object_name = object_filepath,
            filename = f
        )
        msg = f'success upload {f}'
        with open(log_path,'a') as fs :
            fs.write(msg+'\n')

        os.remove(f)
        # upload_file_list.append(object_filepath)



get_regions = PythonOperator(
    task_id='get_regions',
    python_callable= _crawling_task,
    provide_context=True,
    # op_kwargs= {'cityname':'인천시','gu':'연수구','dong':'송도동'},
    dag= dag)

merge_files = PythonOperator(
    task_id = 'merge_file',
    python_callable = _merge_daily_file,
    provide_context=True,
    dag = dag
)


upload_files = PythonOperator(
    task_id = 'upload_file',
    python_callable = _upload_file,
    dag = dag
)

# p = 'estate/songdo/20230406_송도동_e편한세상송도.csv'
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
    sql = f"insert into {project}.{dataset}.{bigquery_agg_table} (confirmYmd,apt_name,count) \
        SELECT confirmYmd,apt_name,sum(1) as count \
        FROM {project}.{dataset}.{bigquery_table} " +
        "where confirmYmd={{ ds_nodash }} group by apt_name,confirmYmd;",
    write_disposition = "WRITE_TRUNCATE",
    use_legacy_sql=False,
    dag=dag
    )
    



get_regions >> merge_files >> upload_files >> gcsToBigQuery >> daily_for_sale_agg