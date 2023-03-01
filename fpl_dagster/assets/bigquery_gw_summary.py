from dagster import asset
from google.cloud import bigquery
import os
from resources.gcs_parquet_io_manager import SEASON

project_ID = os.getenv('PROJECT_ID')

@asset(non_argument_deps = {'gw_summary'},required_resource_keys={"bq_res", "google_config"})
def bigquery_gw_summary(context) -> None:
    """Loads all gw_summary Parquet files into BigQuery Table"""
    
    gc_config = context.resources.google_config
    #Define job config for BQ, note we overwrite the existing table

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET, write_disposition="WRITE_TRUNCATE"
    )

    #uri to batch load all of the parquet files in the gw_summary directory
    batch_uri = f'gs://{gc_config["bucket"]}/{SEASON}/gw_summary/*.parquet'

    load_job = context.resources.bq_res.load_table_from_uri(
        batch_uri, 
        f'{project_ID}.{gc_config["dataset"]}.{SEASON}_gw_summary',
        job_config=job_config
    ) 

    return None