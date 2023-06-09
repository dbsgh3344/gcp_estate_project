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
    dag_id= "crawling_songdo_apt",
    description= "crawling songdo apt info",
    start_date= datetime.datetime(2023,5,10,tzinfo= time_z),
    schedule_interval= "@daily",
    # schedule_interval= "@hourly",
    # schedule_interval=None,
    catchup=False
)


def _crawling_task(ds_nodash,**kwargs) :
    c = CrawlingEstatesInfo('new.land.naver.com',ds_nodash)
    c.get_specific_region(kwargs['cityname'],kwargs['gu'],kwargs['dong'])

# def _crawling_task_yeonsu(ds_nodash,**kwargs) :
#     c = CrawlingEstatesInfo('new.land.naver.com',ds_nodash)
#     c.get_specific_region('인천시','연수구','연수동')




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
    tmp_path = os.path.join(cur_path,'transdata')
    filelist = glob.glob(tmp_path+'/*.csv')

    tmp = pd.DataFrame()
    for f in filelist :
        dt = os.path.basename(f).split('_')[0]
        if dt == ds_nodash:
            df = pd.read_csv(f)
            tmp = pd.concat([tmp,df])
            os.remove(f)
            
    
    if len(tmp) :
        tmp.drop_duplicates(subset=['no'],keep='last',inplace=True)
        tmp.dropna(inplace=True)
        tmp = tmp.astype(col_type)
        filename = os.path.join(tmp_path,f'apt_{ds_nodash}.csv')
        tmp.to_csv(filename,index=False)
    else :
        raise AirflowSkipException(f"No Data on {ds_nodash}")



def _upload_file():
    gcs = GCSHook()
    # bk = 'estate_bucket'
    cur_path = os.path.dirname(os.path.realpath(__file__))
    tmp_path = os.path.join(cur_path,'transdata')
    filelist = glob.glob(os.path.join(tmp_path,'*'))
    log_path = os.path.join(cur_path,'logs','file_upload.txt')
    # daily_dir = datetime.datetime.now().strftime('%Y%m%d')

    for f in filelist:
        file_dt = os.path.basename(f).split('_')[0]
        object_filepath = f'estate/incheon/{os.path.basename(f)}'
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


start_task = DummyOperator(
    task_id = 'start',
    queue='server01',
    dag = dag
)

# crawling_data_1 = PythonOperator(
#     task_id='get_data_1',
#     python_callable= _crawling_task_songdo,
#     provide_context=True,
#     # op_kwargs= {'cityname':'인천시','gu':'연수구','dong':'송도동'},
#     dag= dag)

# crawling_data_2 = PythonOperator(
#     task_id='get_data_2',
#     python_callable= _crawling_task_yeonsu,
#     provide_context=True,
#     dag= dag)

merge_files = PythonOperator(
    task_id = 'merge_file',
    python_callable = _merge_daily_file,
    provide_context=True,
    queue='server01',
    dag = dag
)

worker_num = Variable.get('worker_num')
regions = Variable.get("region_json",deserialize_json=True)
cur_path = os.path.dirname(os.path.realpath(__file__))
for i,dong in enumerate(list(regions['dong'])) :
    cityname = regions['cityname']
    gu = regions['gu']
    # dong = list(regions['dong']).pop()
    worker_idx = (i%int(worker_num)) + 1 
    crawling_data = PythonOperator(
        task_id=f'get_data_{i+1}',
        python_callable= _crawling_task,
        provide_context=True,
        op_kwargs= {'cityname':cityname,'gu':gu,'dong':dong},
        queue = f"server{str(worker_idx).zfill(2)}",
        dag= dag
        )

    trans_data = SFTPOperator(
        task_id = f'trans_data_{i+1}',
        ssh_conn_id = f'ssh_worker_1',
        local_filepath = f"{cur_path}/testdata/"+"{{ ds_nodash }}"+f"_{dong}.csv",
        remote_filepath = f"/home/dbsgh3322/estate_project/dags/transdata/"+"{{ ds_nodash }}"+f"_{dong}.csv",
        operation = "put",
        queue = f"server{str(worker_idx).zfill(2)}",
        dag=dag
    )

    start_task >> crawling_data >> trans_data >> merge_files
    





upload_files = PythonOperator(
    task_id = 'upload_file',
    python_callable = _upload_file,
    queue='server01',
    dag = dag
)

del_today_data_in_bq = BigQueryExecuteQueryOperator(
    task_id = "del_today_in_bq",
    gcp_conn_id = "bigquery_default",
    sql = [
        f"delete {project}.{dataset}.{bigquery_table} where " +
        "confirmYmd={{ ds_nodash }}"
    ],
    write_disposition = "WRITE_TRUNCATE",
    use_legacy_sql=False,
    queue='server01',
    dag=dag
)

gcsToBigQuery = GoogleCloudStorageToBigQueryOperator(
    task_id = 'gcs_to_bq', 
    gcp_conn_id = 'bigquery_default',
    destination_project_dataset_table = f'{dataset}.{bigquery_table}', 
    bucket = gcs_bucket, 
    source_objects = ["estate/incheon/*_{{ ds_nodash }}.csv"],
    source_format = 'CSV',
    write_disposition='WRITE_APPEND',
    # create_disposition = 'CREATE_IF_NEEDED',
    queue='server01',
    dag=dag
)

daily_for_sale_agg = BigQueryExecuteQueryOperator(
    task_id = "daily_for_sale_agg",
    gcp_conn_id = "bigquery_default",
    sql = [f"delete {project}.{dataset}.{bigquery_agg_table} where " + 
        "confirmYmd={{ ds_nodash }};",
        f"insert into {project}.{dataset}.{bigquery_agg_table} (confirmYmd,apt_name,count) \
        SELECT confirmYmd,apt_name,sum(1) as count \
        FROM {project}.{dataset}.{bigquery_table} " +
        "where confirmYmd={{ ds_nodash }} group by apt_name,confirmYmd;"],
    write_disposition = "WRITE_TRUNCATE",
    use_legacy_sql=False,
    queue='server01',
    dag=dag
    )
    

# start_task >> [crawling_data_1,crawling_data_2] >> merge_files >> upload_files >> del_today_data_in_bq >> gcsToBigQuery >> daily_for_sale_agg
merge_files >> upload_files >> del_today_data_in_bq >> gcsToBigQuery >> daily_for_sale_agg