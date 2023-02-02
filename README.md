# Fantasy Premier League with Dagster

## Project Summary

This repository contains a dagster implementation which loads Fantasy Premier League (FPL) data using the FPL api endpoints to Google Cloud Storage in Parquet format. Gameweek data is then loaded into BigQuery ready for analysis and visualisation using Superset.


## Motivation

Quite a few of my friends participate in Fantasy Premier League (FPL) each season where the winner of their mini-league wins some money and more importantly, bragging rights. While I am not participating this season, I thought now would be a good time to start preparing for the next season. I am going to need all the help I can get to win so I thought I should make use of the data that FPL provides through numerous api endpoints to supplement my "ball" knowledge. Also since I am a Data Engineer I thought it would be a good opportunity to develop my skills with Dagster, an open source orchestrator, and docker. 


## Goals

The goal for designing this project was to be able to retrieve the performance of all players, including players in my team, for each gameweek to track the performance of my team and other players. This would hopefully allow me to identify any underperforming players in my team and swap them in for players performing well. To do this I need the stats of each player for each completed gameweek with the players which are in my team for that gameweek flagged.


## Design

All of the data we need is not available in a single endpoint and so we have to combine data from various endpoints to obtain our desired outcome. The first data we need is a list of all of the active players in the Premier League with their unique id, name and team name. This is obtained from our first asset "player_info" and is stored as a single parquet file in GCS. Note that this can only really change during transfer periods or if a new player is rotated into a Premier league squad. We do not need to keep track of the history of this file so we can overwrite the output each time.

Secondly we need the stats of all of the players for each of the completed gameweeks. This is created from the second asset and builds upon the first asset as we need to merge on the player id to get player name and team name. Additionally we add markers for players that were in our team for that specific gameweek. The second asset is partitoned on the Gameweek so we can simply select a gameweek or range of partitions when we need new data.

The design of this project can be summarised into these simple steps:

1. Load player data such as id, name and team name onto GCS in a Parquet file.
2. Merge player info with gameweek player data  such as goals_scored, minutes, etc and store as Parquet files on GCS (One file per gameweek).
3. Create a BigQuery Table from all of the gameweek Parquet files.
4. Use Looker or Superset to analyse the data. 

## Dagster

For those of you unfamiliar Dagster, it is an open source orchestrator I was interested in becoming more familiar with it mainly because of its declarative approach.  This declarative approach is enabled by the Software-defined asset, or asset for short, which is an object in persistent storage such as a Parquet file or database table. The asset lineage for my project can be seen below. 

![Dagster asset lineage](https://user-images.githubusercontent.com/99501368/216422091-b32742a7-4ac9-41a0-9841-07fe0f812b6f.PNG)



Data is initially extracted from the api endpoints and then saved as Parquet files in GCS for storage. BigQuery Tables are then created using the Parquet files stored in GCS.


Another focus of Dagster is seperation of code relating to business logic and code relating to the storage and loading of objects. They do this with the help of IO managers (INPUT LINK HERE). This is in hopes of simplifying the code and allowing you to change the environment easily (different IO manager for dev and prod for example). I have endeavoured to follow this paradigm by implementing my own IO Manager, GCS_Parqeut_IO_Manager which loads pandas dataframes to GCS in parquet format. The file structure in GCS is highly customized to my use case but can easily be altered for your needs. I have added comments in the IO manager on how to do this.

I have not however decided to implement an IO Manager for BigQuery as I am simply loading the table from files already in GCS so I do not need to read to access the files in Dagster. Note I did firstly try to implement an IO manager for BigQuery but it was making the code more complex, and was not worthwhile in terms of development time and so strayed from the seperation of business and I/O logic for my BigQuery assets. 

Partitioning: One of the main difficulties I faced for this project is the fact that the gameweeks do not correspond to weeks of the year as breaks in the Premier League season occur due to cup games, international games and even the World Cup this year. Consequently there was not a simple way for me to partition the assets by week and then simply schedule it similarly. Fortunately we do know how many gameweeks there are, 38, since each team plays 38 games in a season (Some teams occasionally play 2 games in a gameweek due to postponement of an earlier game). I therefore decided to make use of Dagster's StaticPartitions; I paritioned the gameweek summary asset, which retrieves data for a specific gameweek, by gameweek i.e. [1,2, ... ,38]. When I materilise the run I can then select the latest gameweek or run backfills if I have made some change to the business logic. This approach overcomes the main hurdle of gameweeks occuring at irregular intervals but with the downside I have to manually select the desired partition and run it. This is not too much of a hassle as I will have to remember to look at my FPL team for the coming week so I will be reminded to update my data. If I run a partition corresponding to a gameweek that has not occured then no new data will be added. Another benefit of this static partitioning is once this season finishes and a new one starts I can begin rerunning from the first partiton.





## Environment




## Future Work
*    Add files for production deployment for dagit and daemon(dockerfile, docker-compose, dagster.yaml, workspace.yaml) 
*    Add CI/CD
*    Add upcoming fixtures for players each gameweek


## Getting started


