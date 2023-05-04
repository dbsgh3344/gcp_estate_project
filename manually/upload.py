from airflow.providers.google.cloud.hooks.gcs import GCSHook
import os
import datetime
import glob
from google.cloud import bigquery,storage

bk = 'estate_bucket'
def _upload_file():
    gcs = GCSHook()
    cur_path = os.path.dirname(os.path.realpath(__file__))
    tmp_path = os.path.join(cur_path,'../dags/testdata')
    filelist = glob.glob(os.path.join(tmp_path,'*'))
    # log_path = os.path.join(cur_path,'logs','file_upload.txt')
    # daily_dir = datetime.datetime.now().strftime('%Y%m%d')

    for f in filelist:
        file_dt = os.path.basename(f).split('_')[0]
        # print(file_dt,f)
        object_filepath = f'estate/songdo/{file_dt}/{os.path.basename(f)}'
        gcs.upload(
            bucket_name = bk,
            object_name = object_filepath,
            filename = f
        )
        msg = f'success upload {f}'
        # with open(log_path,'a') as fs :
        #     fs.write(msg+'\n')

        os.remove(f)
        # upload_file_list.append(object_filepath)

def insert_to_bq() :
    client = bigquery.Client()
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bk)
    dataset_id = 'tmp'
    table_id = 'test2'
    table_ref = client.dataset(dataset_id).table(table_id)

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
    )

    for i in bucket.list_blobs():
        root = 'estate/songdo/20230502'
        if i.name.startswith(root) :
            file_name = i.name
            uri = 'gs://{}/{}'.format(bk, file_name)

            load_job = client.load_table_from_uri(
                uri, table_ref, job_config=job_config
            )
            load_job.result()  # Wait for the load job to complete

            # # Print the number of rows inserted into the BigQuery table
            # print('Loaded {} rows into {}:{}.'.format(
            #     load_job.output_rows, dataset_id, table_id))
            print(f"{i.name} loaded")



insert_to_bq()
# _upload_file()