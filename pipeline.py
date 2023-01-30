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
from google.cloud import bigquery
from gcs_parquet_io_manager import GCSParquetIOManager
from fpl_api_getter import (
    gw_stats_getter,
    gw_fixture_getter,
    gw_deadline_getter,
    player_getter,
    team_getter
)


gameweek_list = [str(gw) for gw in range(1,39)]

gameweek_partitions = StaticPartitionsDefinition(gameweek_list)

@asset(io_manager_key='gcs_io_manager')
def all_team_stats() -> pd.DataFrame:
    df = pd.DataFrame(team_getter())
    df.rename(columns = {'id': 'team_id','name':'team_name'},inplace = True)

    return df 


@asset(io_manager_key='gcs_io_manager')
def teams_to_join(context, all_team_stats) -> pd.DataFrame:
    return all_team_stats[['team_id','team_name']]




@asset(io_manager_key='gcs_io_manager')
def all_players_stats() -> pd.DataFrame:

    df = pd.DataFrame(player_getter())
    df.rename(columns = {'team':'team_id'}, inplace = True)

    return df

@asset(io_manager_key='gcs_io_manager')
def players_to_join(all_players_stats) -> pd.DataFrame:
    return all_players_stats[['id','first_name','second_name','team_id']]

    

@asset(partitions_def=gameweek_partitions,io_manager_key='gcs_io_manager')
def gw_summary(context, players_to_join, teams_to_join) -> pd.DataFrame:
    '''
    Summary - This asset returns a pd.DataFrame containing all player stats for a specified gameweek

    Returns - Returns a pd.DataFrame containing player summary data for a specific gameweek
    
    '''
    df = pd.DataFrame(gw_stats_getter(context.partition_key))
    #Merge the gw dataframe to the team and player_name dataframes 
    df = df.merge(players_to_join, how = 'left', on ='id' )
    df = df.merge(teams_to_join, how = 'left',on ='team_id')
    #Add a column which indicates the gameweek the dataframe refers too.
    df['gameweek'] = int(context.partition_key)

    return df


@asset(non_argument_deps = {'gw_summary'},required_resource_keys={"bq_res"})
def combined_gw_summary(context) -> None:

    # Big query configuring variables
    project_ID = ''
    bucket_name =''
    dataset_name =''
    
    #Define job config for BQ, note we overwrite the existing table
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_TRUNCATE"
    )

    #uri to load all of the parquet files in the gw_summary directory
    batch_uri = f'gs://{bucket_name}/gw_summary/*.parquet'

    load_job = context.resources.bq_res.load_table_from_uri(
        batch_uri, f'{project_ID}.{dataset_name}.gw_summary', job_config=job_config
    ) 

    return None


@io_manager(required_resource_keys={'gcs'})
def gcs_parquet_io_manager(init_context):
    return GCSParquetIOManager('')

defos = Definitions(
    assets=[gw_summary, combined_gw_summary,all_players_stats, players_to_join,
    all_team_stats,teams_to_join],
    resources={"gcs_io_manager": gcs_parquet_io_manager,"gcs": gcs_resource,
     "bq_res":bigquery_resource})

