# csv_to_bq
import traceback
import os
import pandas as pd 
from io import BytesIO
from pandas.io import gbq 
from google.cloud import storage

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
        
