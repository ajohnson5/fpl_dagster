from dagster import (
    asset,
    Definitions,
    io_manager,
    StaticPartitionsDefinition,
    resource,
    Output
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
    player_getter,
    team_getter
)

#Define season for GCS directory structure
SEASON = '2022'
#Define gameweek partition
gameweek_list = [str(gw) for gw in range(1,39)]
gameweek_partitions = StaticPartitionsDefinition(gameweek_list)

#GCP variables
project_ID = os.environ['PROJECT_ID']
project_dataset = os.environ['PROJECT_DATASET']
project_bucket = os.environ['PROJECT_BUCKET']


@asset(io_manager_key='gcs_io_manager')
def player_info() -> pd.DataFrame:
    """Uses FPL API to create DataFrame containing general player data for all Premier League players
    such as name and team_name"""
    player_df = pd.DataFrame(player_getter())
    team_df = pd.DataFrame(team_getter())

    merged_df = player_df.merge(team_df, how = 'left', on ='team_id')
    return merged_df


@asset(partitions_def=gameweek_partitions,io_manager_key='gcs_io_manager', output_required=False)
def gw_summary(context, player_info) -> pd.DataFrame:
    """
    Statically paritioned asset which creates DataFrame containing gameweek stats for all active players
    in gameweek = partition.

    args 
    - context: context object passed to access partition key for each run
    - player_info: DataFrame containing general player info such as name and team name. 
    """

    #If gameweek has not occured then nothing happends
    if gw_stats_getter(context.partition_key) != []:
        gw_df = pd.DataFrame(gw_stats_getter(context.partition_key))
        gw_df = gw_df.merge(player_info, how = 'left',on = 'id')

        upcoming_fix_df = pd.DataFrame(gw_fixture_getter(int(context.partition_key)))
        team_df = pd.DataFrame(team_getter())


        #Add the 5 upcoming fixtures for each player
        upcoming_fix_df = upcoming_fix_df.merge(team_df, how = 'left',left_on='team_against_id',
            right_on='team_id',suffixes=('','_x'))
        upcoming_fix_df.drop(columns=['team_id_x','team_against_id'], inplace = True)
        upcoming_fix_df.rename(columns={'team_name':'team_to_play'},inplace = True)

        #Group by gw and team_id so if a team has a double gameweek then rows are combined
        #Concatenate opposition and seperate by a comma.
        upcoming_fix_df = upcoming_fix_df.groupby(['gw_to_play','team_id'])['team_to_play'].apply(', '.join).reset_index()


        for i in range(1,6):
            gw_df = gw_df.merge(upcoming_fix_df[upcoming_fix_df['gw_to_play']==i], how = 'left', on='team_id',
                suffixes=('','_x'))
            gw_df.rename(columns={'team_to_play':f'team_to_play_{i}'},inplace = True)
            gw_df.drop(columns=['gw_to_play'],inplace = True)

        yield Output(gw_df)


@asset(non_argument_deps = {'gw_summary'},required_resource_keys={"bq_res"})
def bigquery_gw_summary(context) -> None:
    """Loads all gw_summary Parquet files into BigQuery Table"""
    
    #Define job config for BQ, note we overwrite the existing table
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_TRUNCATE"
    )

    #uri to batch load all of the parquet files in the gw_summary directory
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
    resources={"gcs_io_manager": gcs_parquet_io_manager,"gcs": gcs_resource,"bq_res":bigquery_resource})
