from dagster import asset, Output,StaticPartitionsDefinition
import pandas as pd
import os
import numpy as np
from utils.fpl_getter import (
    gw_stats_getter,
    gw_fixture_getter,
    player_getter,
    team_getter,
    gw_my_fpl_team,
)

manager_ID = os.getenv('MANAGER_ID')

# Define gameweek partition
gameweek_list = [str(gw) for gw in range(1, 39)]
gameweek_partitions = StaticPartitionsDefinition(gameweek_list)

@asset(
    partitions_def=gameweek_partitions,
    io_manager_key="gcs_io_manager",
    output_required=False,
)
def gw_summary(context, player_info) -> pd.DataFrame:
    """
    Statically paritioned asset which creates DataFrame containing gameweek stats for all active players
    in gameweek = partition.

    args
    - context: context object passed to access partition key for each run
    - player_info: DataFrame containing general player info such as name and team name.
    """

    # If gameweek has not occured then nothing happends
    if gw_stats_getter(context.partition_key) != []:
        gw_df = pd.DataFrame(gw_stats_getter(context.partition_key))

        # Convert some stats columns to floats (FPL API stores them as strings)
        cols_to_convert_int = [
            "influence",
            "creativity",
            "threat",
            "ict_index",
            "expected_goals",
            "expected_assists",
            "expected_goal_involvements",
            "expected_goals_conceded",
        ]
        gw_df[cols_to_convert_int] = gw_df[cols_to_convert_int].apply(pd.to_numeric)

        # Merge gw_df with player_info to get player_name, team_name, etc.
        gw_df = gw_df.merge(player_info, how="left", on="id")

        # Use gw_my_fpl_team func to get info on my team for given gameweek
        my_fpl_team = gw_my_fpl_team(context.partition_key, manager_ID)

        # If there exists no data for my team on specified gameweek then set columns to be null
        # Otherwise merge resulting dataframe to gw_df dataframe.
        if my_fpl_team is None:
            gw_df[
                ["my_team_position", "multiplier", "is_captain", "is_vice_captain"]
            ] = np.NaN
        else:
            my_fpl_team_df = pd.DataFrame(my_fpl_team)
            gw_df = gw_df.merge(my_fpl_team_df, how="left", on="id")

        # Get this gameweeks upcoming fixtures for next 5 gameweeks.
        upcoming_fix_df = pd.DataFrame(gw_fixture_getter(int(context.partition_key)))
        team_df = pd.DataFrame(team_getter())

        # Merge upcoming fixtures to team_names to get names of opposition teams
        upcoming_fix_df = upcoming_fix_df.merge(
            team_df,
            how="left",
            left_on="team_against_id",
            right_on="team_id",
            suffixes=("", "_x"),
        )
        upcoming_fix_df.drop(columns=["team_id_x", "team_against_id"], inplace=True)
        upcoming_fix_df.rename(columns={"team_name": "team_to_play"}, inplace=True)

        # Group by gw and team_id so if a team has a double gameweek then rows are combined
        # Concatenate opposition team names for double games weeks (seperated by comma)
        upcoming_fix_df = (
            upcoming_fix_df.groupby(["gw_to_play", "team_id"])["team_to_play"]
            .apply(", ".join)
            .reset_index()
        )

        # Add upcoming fixtures for each player (iterate through upcoming gameweeks)
        for i in range(1, 6):
            gw_df = gw_df.merge(
                upcoming_fix_df[upcoming_fix_df["gw_to_play"] == i],
                how="left",
                on="team_id",
                suffixes=("", "_x"),
            )
            gw_df.rename(columns={"team_to_play": f"team_to_play_{i}"}, inplace=True)
            gw_df.drop(columns=["gw_to_play"], inplace=True)

        yield Output(gw_df)