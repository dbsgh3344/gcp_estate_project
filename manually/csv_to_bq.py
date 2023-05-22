# csv_to_bq
import traceback
import os
import pandas as pd 
from io import BytesIO
from pandas.io import gbq 
from google.cloud import storage
import glob

client = storage.Client()
bucket = client.get_bucket('estate_bucket')


p_id= 'estate-project-382208'
d = {
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

            
def csv_to_bq():
    for i in bucket.list_blobs() :
        root = 'estate/songdo/20230504'
        # re_root = 'estate/songdo/20230425'
        if i.name.startswith(root) :    # find csv file in gcs
            print(i.name)
            b= i.download_as_string() 
            csv = BytesIO(b) 
            df = pd.read_csv(csv) # byte to dataframe
            df.dropna(inplace=True)
            df= df.astype(d)
            # print(df.head())
            table_name = 'tmp.test2'
            gbq.to_gbq(df, table_name, project_id=p_id, if_exists='append') # insert df to bq


def merge_gcs_data(dt) :
    tmp_local_path = '/home/song/code/python_dir/tmpdir'
    tmp = pd.DataFrame()
    cnt = 0
    for i in bucket.list_blobs() :
        if i.name.startswith(f'estate/songdo/{dt}'):
            b= i.download_as_string() 
            csv = BytesIO(b) 
            df = pd.read_csv(csv) # byte to dataframe
            cnt+=len(df)
            tmp = pd.concat([tmp,df])
            tmp.drop_duplicates(subset=['no'],keep='last',inplace=True)
            tmp.dropna(inplace=True)
            tmp = tmp.astype(d)
    
    print(cnt, len(tmp))
    path = os.path.join(tmp_local_path,f'songdo_{dt}.csv')
    tmp.to_csv(path,index=False)
    
def upload_local_to_gcs() :
    tmp_local_path = '/home/song/code/python_dir/tmpdir'
    filelist = glob.glob(tmp_local_path+'/*.csv')
    for f in filelist :
        blob = bucket.blob(f'estate/songdo/{os.path.basename(f)}')
        blob.upload_from_filename(f)
        print(f"upload {f}")
        os.remove(f)

        
if __name__ == "__main__" :
    # prp_gcs_csv_type()
    # for date in range(1,18):
    #     cnvt_date = f"202305{str(date).zfill(2)}"
    #     merge_gcs_data(cnvt_date)
    upload_local_to_gcs()