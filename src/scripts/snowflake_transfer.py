import pandas as pd
import logging
import json
import datetime as dt
import time
import os
import boto3
from secrets_access import get_secret
from typing import List, Any, Optional
from logger import logger
from connectors import get_snowflake_connection, s3_conn
from snowflake.connector import ProgrammingError


# Data Source: https://www.hockey-reference.com/leagues/NHL_2022.html ##
# TODO: S3 connection has been established. Now, the data needs to be moved from S3
#  into Snowflake from the existing stages.
#       So:
#           1) Create connection in code to Snowflake that will create the schema and upload the raw data.
#             2) Establish a Snowpark session to transform the data and upload it into a clean schema.
#               3) Automate these processes in steps 1 & 2 with MWAA.


# LOGGING
logger = logger('snowflake_transfer')


# QUERY EXECUTIONS
snowflake_schema_queries = {
    "team_stats": """
        create or replace table team_stats (
        Rk integer,
        Team varchar(100) PRIMARY KEY not null,
        AvAge integer,
        GP integer,
        W integer,
        L integer,
        OL integer,
        PTS integer,
        PTS_PERC float,
        GF integer,
        GA integer,
        SOW integer,
        SOL integer,
        SRS integer,
        SOS integer,
        GFVG integer,
        GAVG integer,
        PP integer,
        PPO integer,
        PP_PERC float,
        PPA integer,
        PPOA integer,
        PK_PERC float,
        SH integer,
        SHA integer,
        PIMVG integer,
        oPIMVG integer,
        S integer,
        S_PERC float,
        SA integer,
        SV_PERC float,
        SO integer
    )
    """,
    "raw_team_stats": """
        create or replace table raw_team_stats
        (
            Rk integer,
            Team varchar(100) PRIMARY KEY not null,
            AvAge integer,
            GP integer,
            W integer,
            L integer,
            OL integer,
            PTS integer,
            PTS_PERC float,
            GF integer,
            GA integer,
            SOW integer,
            SOL integer,
            SRS integer,
            SOS integer,
            GFVG integer,
            GAVG integer,
            PP integer,
            PPO integer,
            PP_PERC float,
            PPA integer,
            PPOA integer,
            PK_PERC float,
            SH integer,
            SHA integer,
            PIMVG integer,
            oPIMVG integer,
            S integer,
            S_PERC float,
            SA integer,
            SV_PERC float,
            SO integer
        )
    """,
    "regular_season": """
        create or replace table regular_season (
            date date,
            away_team varchar(100),
            away_goals integer,
            home_team varchar(100),
            home_goals integer,
            length_of_game_min integer,
            away_outcome integer,
            home_outcome integer
        )
    """,
}
snowflake_raw_ingest = {
    # RAW TEAM STATISTICS. USES THE S3 INTEGRATION STAGE FOR THE S3 RAW DATA.
    "team_stats_raw": """
        copy into raw_team_stats
        from @nhl_raw_data/teams
        file_format=csv
        pattern = '.*csv.*'
        -- ON_ERROR = CONTINUE
    """,

    # REGULAR SEASON DATA CLEAN. USES THE S3 INTEGRATION STAGE FOR THE S3 RAW DATA.
    "reg_season_cleaned": """
            copy into regular_season
            from @nhl_clean_data/season
            file_format=csv
            pattern = '.*csv.*'
    """
}
snowflake_cleaned_ingest = {
    # TEAM STATISTICS DATA CLEANED. USES THE S3 INTEGRATION STAGE FOR THE S3 RAW DATA.
    "team_stats_cleaned": """
    copy into team_stats
    from @nhl_clean_data/teams
    file_format=csv
    pattern = '.*csv.*'
"""
}


def file_parser(path):
    dataframes = []
    filenames = []
    path = os.path.join(path)

    for root, dirs, files in os.walk(path, topdown=False):
        print(root)
        print(dirs)
        print(files)
        for name in files:
            filenames.append(name)
            df = pd.read_csv(os.path.join(root, name))
            dataframes.append(df)


@s3_conn
def s3_parser(s3_buckets):

    # Establish connection
    s3_client, s3_resource = boto3.client('s3'), boto3.resource('s3')

    my_bucket = s3_resource.Bucket(s3_buckets)
    for my_bucket_object in my_bucket.objects.all():
        logger.info(my_bucket_object.key)


def snowflake_query_exec(queries, method='standard'):
    try:

        # SNOWFLAKE CONNECTION
        conn = get_snowflake_connection(method)

        # Cursor
        curs = conn.cursor()
        response = {}
        for idx, query in queries.items():
            curs.execute_async(query)
            query_id = curs.sfqid
            logger.info(f'Query added to queue: {query_id}')

            curs.get_results_from_sfqid(query_id)

            # IF THE SNOWFLAKE QUERY RETURNS DATA, STORE IT. ELSE, CONTINUE PROCESS.
            result = curs.fetchone()
            if result:
                logger.info(f'Query completed successfully and stored: {query_id}')
                response[idx] = result[0]
            else:
                pass

            while conn.is_still_running(conn.get_query_status_throw_if_error(query_id)):
                logger.info(f'Awaiting query completion for {query_id}')
                time.sleep(1)

        return response

    except ProgrammingError as err:
        logger.error(f'Programming Error: {err}')


# CREATE SCHEMA
schema = snowflake_query_exec(snowflake_schema_queries, method='standard')

# INGEST RAW DATA
snowflake_query_exec(snowflake_raw_ingest, method='standard')

# TRANSFORM RAW DATA
# spark_manipulation = snowflake_query_exec(queries, method='spark')

# PARSE RAW DATA FROM S3
s3 = s3_parser('nhl-data-raw')

# PARSE RAW DATA FROM STORAGE

# Paths
seasons, team_stats = os.path.join('/Users/rschraeder/Desktop/Projects/StanleyCupPredictions/data/seasons/'), os.path.join(
    '/Users/rschraeder/Desktop/Projects/StanleyCupPredictions/data/teams/')

seasons = file_parser(seasons)
team_stats = file_parser(team_stats)

# games_df = games_df.rename(columns=({
#     'Date': 'date', 'Visitor': 'away_team', 'Home': 'home_team', 'G': 'away_goals', 'G.1': 'home_goals', 'LOG': 'length_of_game_min'
# }))
# games_df = games_df[['date', 'away_team', 'away_goals', 'home_team', 'home_goals', 'length_of_game_min']]
#
# # Transforming data
# games_df['length_of_game_min'] = [i.replace(':', '') for i in games_df['length_of_game_min']]
# games_df['length_of_game_min'] = [(int(i[0]) * 60) + int(i[1:]) for i in games_df['length_of_game_min']]
#
# games_df.date = games_df.date.apply(pd.to_datetime)
#
#
# def encoding_game_outcome(dataset, away_output_colname: str, home_output_colname: str, away_goals: str,
#                           home_goals: str) -> List[int]:
#     dataset[f'{away_output_colname}'] = (dataset[f'{away_goals}'] - dataset[f'{home_goals}']).apply(
#         lambda x: 1 if x > 0 else 0)
#     dataset[f'{home_output_colname}'] = (dataset[f'{home_goals}'] - dataset[f'{away_goals}']).apply(
#         lambda x: 1 if x > 0 else 0)
#
#     return dataset
#
#
# games_df = encoding_game_outcome(games_df, 'away_outcome', 'home_outcome', 'away_goals', 'home_goals')
#
# # Output to CSV
# games_df.to_csv(os.path.join(path, 'regular_season_clean.csv'), index=False)
#
# # Team name cleaning
# team_stats_df['Team'] = [str(i).replace('*', '') for i in team_stats_df['Team']]
#
# # Creating Column for Total Goals
#
# team_stats_df['G'] = team_stats_df.GF + team_stats_df.GA
#
# # Creating Column for Total Power-Play Goals
#
# team_stats_df['PPG'] = team_stats_df.PP + team_stats_df.PPA
#
# # Creating Column for Total Games in Shootouts
#
# team_stats_df['SHOOTOUTS'] = team_stats_df.SOW + team_stats_df.SOL
#
#
# def percents(df):
#     for column, row in df.iteritems():
#         if '%' in column:
#             for item in row:
#                 if item < 1:
#                     row += row * 100
#
#     return df
#
#
# team_stats_df = percents(team_stats_df)
#
# # Output to CSV
# team_stats_df.to_csv(os.path.join(path, 'team_stats_clean.csv'), index=False)