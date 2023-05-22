from airflow import DAG,macros
import pandas as pd
import sys
sys.path.append('/home/dbsgh3322/estate_project/extract_data/')
from crawling_estates import CrawlingEstatesInfo
import datetime
import pendulum
import glob
import os
from airflow.operators.python import PythonOperator

time_z = pendulum.timezone('Asia/Seoul')
dag = DAG(
    dag_id= "backfill_test",
    description= "backfill previous data",
    start_date= datetime.datetime(2023,5,10,tzinfo= time_z),
    schedule_interval= "@daily",
    # schedule_interval= "@hourly",
    # schedule_interval=None
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
    if len(tmp)!=0:
        tmp.drop_duplicates(subset=['no'],keep='last',inplace=True)
        tmp.dropna(inplace=True)
        tmp = tmp.astype(col_type)
        filename = os.path.join(tmp_path,f'songdo_{ds_nodash}.csv')
        tmp.to_csv(filename,index=False)
    else :
        print('file not exist')

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

get_regions >> merge_files