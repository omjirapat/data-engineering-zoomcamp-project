from dagster import asset
import pandas as pd
import requests
import io
from google.cloud import storage

@asset(
    group_name="ga_retail_event",
    compute_kind="python",
    description="This asset download data from kaggle."
)
def load_data_and_upload_to_gcs() -> str:
    urls= [
        'https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz'
        #ref https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data
    ]
    dfs = []
    for url in urls:
        df = pd.read_csv(url, sep=",", compression='gzip')
        dfs.append(df)
    retail_data = pd.concat(dfs, ignore_index=True)
    bucket_name = 'retailer_datalake_datacafeplayground'
    file_name = 'retail_data.parquet'
    gcs_path = f'gs://{bucket_name}/{file_name}'
    retail_data.to_parquet(file_name, index=False)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_filename(file_name)
    return gcs_path