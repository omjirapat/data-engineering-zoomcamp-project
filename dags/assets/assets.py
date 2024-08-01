import pandas as pd
import os
from dotenv import load_dotenv
from dagster import asset
from google.cloud import storage,bigquery
load_dotenv()
bucket_name = os.environ.get("BUCKET")
table_name = os.environ.get("TABLE_NAME")

@asset(
    group_name="ga_retail_event",
    compute_kind="python",
    description="This asset download data from kaggle.",
)
def load_data_and_upload_to_gcs() -> str:
    urls = [
        "https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz"
        # ref https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store/data
    ]
    dfs = []
    for url in urls:
        df = pd.read_csv(url, sep=",", compression="gzip")
        dfs.append(df)
    retail_data = pd.concat(dfs, ignore_index=True)
    file_name = "retail_data.parquet"
    gcs_path = f"gs://{bucket_name}/{file_name}"
    retail_data.to_parquet(file_name, index=False)
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_filename(file_name)
    return gcs_path

@asset(
    group_name="ga_retail_event",
    compute_kind="python",
    description="create external table on bigquery.",
)
def create_bigquery_external_table(load_data_and_upload_to_gcs):
    bigquery_client = bigquery.Client()
    ddl = f"""
        CREATE OR REPLACE EXTERNAL TABLE `{table_name}`
    (
        event_time STRING,
        event_type STRING,
        product_id INT64,
        category_id INT64,
        category_code STRING,
        brand STRING,
        price FLOAT64,
        user_id INT64,
        user_session STRING
    )
    OPTIONS (
        format = 'parquet',
        uris = ['gs://{bucket_name}/*']
    );
    """
    query_job = bigquery_client.query(ddl)
    query_job.result() 
    return None
#test 1,2