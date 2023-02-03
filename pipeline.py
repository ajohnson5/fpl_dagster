from dagster import (
    asset,
    Definitions,
    io_manager,
    StaticPartitionsDefinition,
    resource
)
from dagster_gcp.gcs import gcs_resource
from dagster_gcp import bigquery_resource
import pandas as pd
import os
from google.cloud import bigquery
from gcs_parquet_io_manager import GCSParquetIOManager
from fpl_getter import (
    gw_stats_getter,
    gw_fixture_getter,
    gw_deadline_getter,
    player_getter,
    team_getter
)

SEASON = '2022'
gameweek_list = [str(gw) for gw in range(1,39)]
gameweek_partitions = StaticPartitionsDefinition(gameweek_list)

project_ID = os.environ['PROJECT_ID']
project_dataset = os.environ['PROJECT_DATASET']
project_bucket = os.environ['PROJECT_BUCKET']


@asset(io_manager_key='gcs_io_manager')
def player_info() -> pd.DataFrame:

    player_df = pd.DataFrame(player_getter())
    team_df = pd.DataFrame(team_getter())

    merged_df = player_df.merge(team_df, how = 'left', on ='team_id')
    

    return merged_df


@asset(partitions_def=gameweek_partitions,io_manager_key='gcs_io_manager')
def gw_summary(context, player_info) -> pd.DataFrame:
    '''
    Summary - This asset returns a pd.DataFrame containing all player stats for a specified gameweek

    Returns - Returns a pd.DataFrame containing player summary data for a specific gameweek
    
    '''
    gw_df = pd.DataFrame(gw_stats_getter(context.partition_key))

    gw_df = gw_df.merge(player_info, how = 'left',on = 'id')

    return gw_df


@asset(non_argument_deps = {'gw_summary'},required_resource_keys={"bq_res"})
def bigquery_gw_summary(context) -> None:

    # Big query configuring variables
    
    #Define job config for BQ, note we overwrite the existing table
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_TRUNCATE"
    )

    #uri to load all of the parquet files in the gw_summary directory
    batch_uri = f'gs://{project_bucket}/{SEASON}/gw_summary/*.parquet'

    load_job = context.resources.bq_res.load_table_from_uri(
        batch_uri, f'{project_ID}.{project_dataset}.{SEASON}_gw_summary', job_config=job_config
    ) 

    return None



@io_manager(required_resource_keys={'gcs'})
def gcs_parquet_io_manager(init_context):
    return GCSParquetIOManager(project_bucket, season = SEASON)


defos = Definitions(
    assets=[player_info, gw_summary, bigquery_gw_summary],
    resources={"gcs_io_manager": gcs_parquet_io_manager,"gcs": gcs_resource,
     "bq_res":bigquery_resource})
