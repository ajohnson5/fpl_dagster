from dagster import asset
import pandas as pd
from utils.fpl_getter import player_getter, team_getter

@asset(io_manager_key="gcs_io_manager")
def player_info() -> pd.DataFrame:
    """Uses FPL API to create DataFrame containing general player data for all Premier League players
    such as name and team_name"""
    player_df = pd.DataFrame(player_getter())
    team_df = pd.DataFrame(team_getter())

    merged_df = player_df.merge(team_df, how="left", on="team_id")
    return merged_df