from google.cloud import bigquery
from google.cloud import storage
import pandas as pd 
from io import BytesIO
from pandas.io import gbq


class ProcessingBQ:

    def __init__(self) -> None:
        self.client = bigquery.Client()
        self.st_client = storage.Client()
        self.bucket_name = 'estate_bucket'
        self.bucket = self.st_client.get_bucket(self.bucket_name)
        self.project = 'estate-project-382208'
        self.dataset = 'estate_dataset'
        self.table = 'for_sale'
        

    def create_table(self):
        # TODO(developer): Set table_id to the ID of the table to create.
        table_id = f"{self.project}.{self.dataset}.{self.table}"

        schema = [
            bigquery.SchemaField("construction_company", "STRING", mode="REQUIRED",),
            bigquery.SchemaField("use_approve", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("detail_addrs", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("addrs", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("apt_code", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("no", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("apt_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("estate_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("trade_name", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("supply_area", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("exclusive_area", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("direction", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("confirmYmd", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("latitude", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("longitude", "FLOAT", mode="REQUIRED"),
            bigquery.SchemaField("price", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("total_floor", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("current_floor", "STRING", mode="REQUIRED"),
            
        ]
        table_obj = bigquery.Table(table_id, schema=schema,)
        table_obj = self.client.create_table(table_obj)  # Make an API request
        print(
            "Created table {}.{}.{}".format(table_obj.project, table_obj.dataset_id, table_obj.table_id)
        )


    def insert_gcs_data_to_bq(self) :
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,
        )

        table_ref = self.client.dataset(self.dataset).table(self.table)
        
        
        for f in self.bucket.list_blobs() :
            file_name = f.name
            if '.csv' in file_name :
                uri = 'gs://{}/{}'.format(self.bucket_name, file_name)
                

                load_job = self.client.load_table_from_uri(
                    uri, table_ref, job_config=job_config
                )
                load_job.result()  # Wait for the load job to complete

                
                # print('Loaded {} rows into {}:{}.'.format(
                #     load_job.output_rows, dataset_id, table_id))
                print(f"{file_name} loaded")

    def insert_csv_to_bq(self,date) :
        # p_id= 'estate-project-382208'

        root = f'estate/songdo/{date}'
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

        for i in self.bucket.list_blobs() :
            # root = 'estate/songdo/20230504'
            # re_root = 'estate/songdo/20230425'
            if i.name.startswith(root) :    # find csv file in gcs
                print(i.name)
                b= i.download_as_string() 
                csv = BytesIO(b) 
                df = pd.read_csv(csv) # byte to dataframe
                df.dropna(inplace=True)
                df= df.astype(d)
                # print(df.head())
                table_name = f'{self.dataset}.{self.table}'
                gbq.to_gbq(df, table_name, project_id=self.project, if_exists='append') # insert df to bq


if __name__ == '__main__' :
    pbq = ProcessingBQ()
    # pbq.create_table()
    # pbq.insert_gcs_data_to_bq()
    pbq.insert_csv_to_bq('20230410')
