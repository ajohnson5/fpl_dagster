# Fantasy Premier League with Dagster

## Project Summary

This repository contains a dagster implementation which loads Fantasy Premier League (FPL) data using the FPL api endpoints to Google Cloud Storage in parquet format. Gameweek data is then loaded into BigQuery ready for analysis and visualisation using Superset.

## Motivation

Quite a few of my friends participate in Fantasy Premier League (FPL) each season where the winner of their mini-league wins some money and more importantly, bragging rights. While I am not participating this season, I thought now would be a good time to start preparing for next season. I am going to need all the help I can get to win so I thought I should make use of the data that FPL provide through numerous api endpoints to supplement my ball knowledge. Also since I am a Data Engineer I thought it would be a good opportunity to develop my skills with Dagster, an open source orchestrator. 


## Contents

*   Dagster
*   Requirements
*   Environment
*   Future Work



## Dagster

For those of you unfamiliar Dagster, it is an open source orchestrator I was interested in becming more familiar mainly because of its declarative approach. Note I am new to Dagster and while I am trying my best to follow best practices I cannot promise the solution I have created is "optimal". Also note that this is a work in progress.

![Dagster Asset Lineage](https://user-images.githubusercontent.com/99501368/216422091-b32742a7-4ac9-41a0-9841-07fe0f812b6f.PNG)

## Requirements

To ensure that the scope of this project is contained and fulfills its purpose I wanted to establish some requirements from the pipeline in terms of its functionality and data.



*   Functionality Requirements

    1. Pipeline should allow backfills for previous gameweeks
    2. Allow the pipeline to be re-run from point-of-failure

*   Data Requirements

    1. Player summary data for all previous gameweeks for all active players should be available in parquet format in GCS and available in BigQuery.
    2. Players summary data for all previous gameweeks for players in my team should be available in parquet format in GCS and available in BigQuery.


The functionality requirements are to ensure that the pipeline is somewhat user friendly and resilient to change.


The data requirements are to ensure that the resulting data is useful and can provide some insight that I can use when making decisions for FPL. These requirements are what I believe will help me make decisions.



---

## Environment


---


## Future Work



