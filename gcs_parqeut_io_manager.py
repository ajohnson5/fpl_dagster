import os
import pandas as pd
from dagster import (
    Field,
    InputContext,
    IOManager,
    OutputContext,
    _check as check,
    io_manager,
)
import gcsfs

# from dagster._utils.backoff import backoff
# from google.api_core.exceptions import Forbidden, ServiceUnavailable, TooManyRequests
from google.cloud import storage

class GCSParquetIOManager(IOManager):

    '''
    Custom IO Manager which stores parquet files in GCS and takes data in as a 
    dataframe. 
    
    '''
    def __init__(self, bucket_name:str, prefix="", season = "2022"):
        self.bucket_name = bucket_name
        self.prefix = prefix
        self.season = season


    # Gets the url of parquet file we will be storing or loading
    def _get_gcs_url(self, context):

        if context.has_partition_key and context.has_asset_partitions:
            file_name = f"{context.asset_key.path[-1]}_{context.asset_partition_key}"
        else:
            file_name = context.asset_key.path[-1]  
        name = context.asset_key.path[-1]  
        self.gs_uri = f"gs://{self.bucket_name}/{self.season}/{name}/{self.prefix}{file_name}.parquet"
        return self.gs_uri

    # This method loads the parquet file to gcs
    def handle_output(self, context, df:pd.DataFrame):
        if df is None:
            return
        
        if not isinstance(df, pd.DataFrame):
            raise ValueError(r'Expected asset to return a pd.DataFrame; got a {df!r} ')

        file_name = self._get_gcs_url(context)

        df.to_parquet(file_name)

    # This method loads the parquet file into the next asset.
    def load_input(self, context) -> pd.DataFrame:

        df = pd.read_parquet(self._get_gcs_url(context))

        return df
