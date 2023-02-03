# Fantasy Premier League with Dagster

## Project Summary

This repository contains a dagster implementation which loads Fantasy Premier League (FPL) data using the FPL api endpoints to Google Cloud Storage in Parquet format. Gameweek data is then loaded into BigQuery ready for analysis and visualisation using Superset.


## Motivation

Quite a few of my friends participate in Fantasy Premier League (FPL) each season where the winner of their mini-league wins some money and more importantly, bragging rights. While I am not participating this season, I thought now would be a good time to start preparing for the next season. I am going to need all the help I can get to win so I thought I should make use of the data that FPL provides through numerous api endpoints to supplement my "ball" knowledge. Also since I am a Data Engineer I thought it would be a good opportunity to develop my skills with Dagster, an open source orchestrator, and docker. 


## Goals

The goal for designing this project was to be able to retrieve data for all players in the Premier League, including players in my team, for each gameweek to track the performance of my team and other players. This would hopefully allow me to identify any players which need to be transferred out of my team for a better performing player.


## Design

The high-level architecture for this project is discussed in the project summary but an architectural diagram can be seen below also.

![Architectural diagram](https://user-images.githubusercontent.com/99501368/216435714-373d69cc-dc89-432c-86c8-8ff7bc8b87a8.jpg)

The ETL of data is orchestrated by Dagster and is split into assets which represent some persistent storage object such as a Parquet file or database table. These assets can be seen below. This DAG tells us what datasets we want to exist rather than how to create the datasets. This is a declarative approach. 


![Dagster asset lineage](https://user-images.githubusercontent.com/99501368/216422091-b32742a7-4ac9-41a0-9841-07fe0f812b6f.PNG)


### Asset 1: player_info

Uses the API endpoints to collect player data such as unique id, name and team name. Contains all players who have played in the Premier League season so far. This data is stored as a Parquet file in GCS. Throughout the season this data can only increase in size, such as transfers or new players making it into the starting XI. Consequntly we overwrite the parquet file each time the asset is materialized as are not concerned with historical data.

### Asset 2: gw_summary

Asset 2 combines the player info Parquet files with gameweek stats for all active players and stores each gameweek as a Parquet file on GCS.
Additionally we add markers for players that were in our team for that specific gameweek. The second asset is partitoned on the Gameweek so we can simply select a gameweek or range of gameweeks when we need new data or want to backfill due a change in our business logic. More information on this can be seen in the Dagster section below.


### Asset 3: bigquery_gw_summary

Asset 3 creates a BigQuery Table from all of the gw_summary Parquet files combined. This asset depends on the gw_summary asset but no data is passed between these two assets. This is because we have already loaded the gw_summary Parqeut files onto GCS and so we can use BigQuery to load all of the gw_summary Parquet files into a BigQuery Table directly without reading the files again in Python. 


### Looker

Now that the data is in BigQuery we can perform analysis using SQL or use connect to it using Looker and visualise the data.

## Dagster

For those of you unfamiliar Dagster, it is an open source orchestrator I was interested in becoming more familiar with it mainly because of its declarative approach.  This declarative approach is enabled by the Software-defined asset, or asset for short, which is an object in persistent storage such as a Parquet file or database table. 

### IO Managers


Another feature, or opinion of Dagster is sepereation of reading and writing of data from business logic. This is in hope of simplifying the code and enabling easier change of deployment environment. They do this with IO managers which when assigned to assets, tell the asset how to read and write the data. 

In this project I have created an IO Manager, GCSParquetIOManager, which loads pandas dataframes to Parqeut files in GCS. Note that this IOManager also works with partitioned data. This IO manager is customized to my use case and consequently stores the files in a structure suitable to my desired setup but this can easily be changed if you desire.

As mentioned earlier, the bigquery_gw_summary asset, while depending on the gw_summary asset, does not actually load any data from it. Instead, once the gw_summary asset is completed running for all of the desired assets, it creates a BigQuery table from all of the Parquet files. Thus no IO manager is needed and while not best practice, I believe this results in the simplest code. I may however, look into implementing this in the future. 


### Partitioning

One of the main difficulties I faced for this project is the fact that the gameweeks do not correspond to weeks of the year as breaks in the Premier League season occur due to cup games, international games and even the World Cup this year. Consequently there was not a simple way for me to partition the assets by week and then simply schedule them to run weekly. Fortunately we do know how many gameweeks there are, 38, since each team plays 38 games in a season (Some teams occasionally play 2 games in a gameweek due to postponement of an earlier game). I therefore decided to make use of Dagster's StaticPartitions; I paritioned the gameweek summary asset, which retrieves data for a specific gameweek, by gameweek i.e. [1,2, ... ,38]. When I materilise the run I can then select the latest gameweek or run backfills if I have made some change to the business logic. This approach overcomes the main hurdle of gameweeks occuring at irregular intervals but with the downside I have to manually select the desired partition and run it. This is not too much of a hassle as I will have to remember to look at my FPL team for the coming week so I will be reminded to update my data. If I run a partition corresponding to a gameweek that has not occured then no new data will be added. Another benefit of this static partitioning is once this season finishes and a new one starts, I can begin rerunning from the first partiton.





## Environment




## Future Work
*    Add files for production deployment for dagit and daemon(dockerfile, docker-compose, dagster.yaml, workspace.yaml) 
*    Add CI/CD
*    Add upcoming fixtures for players each gameweek


## Getting started
