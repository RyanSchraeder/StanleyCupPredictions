import pandas as pd
import argparse
import requests
from io import StringIO
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
from connectors import get_legacy_session

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
    "raw_team_stats": """
        create or replace table raw_team_stats
        (
            TeamId integer PRIMARY KEY not null,
            Rk integer,
            Team varchar(100),
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
            SO integer, 
            updated_at date
        )
    """,
    # "teams": """
    #     create or replace table teams (
    #         team_id autoincrement start 1 increment 1,
    #         team_name varchar(100),
    #         city varchar(100),
    #         state varchar(100)
    #     )
    # """,
    "regular_season": """
        create or replace table regular_season (
            date date,
            away_team varchar(100), -- inherit team_id
            away_goals integer,
            home_team varchar(100), -- inherit team_id
            home_goals integer,
            length_of_game_min integer,
            away_outcome integer,
            home_outcome integer,
            updated_at varchar(100)
        )
    """,
    "playoff_season": """
        create or replace table playoff_season (
            date date,
            away_team varchar(100),
            away_goals integer,
            home_team varchar(100),
            home_goals integer,
            length_of_game_min integer,
            away_outcome integer,
            home_outcome integer,
            updated_at varchar(100)
        )
    """
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
    "reg_season_raw": """
            copy into regular_season
            from @nhl_raw_data/season
            file_format=csv
            pattern = '.*csv.*'
    """
}


class SnowflakeIngest(object):

    def __init__(self, source, endpoint, s3_path, snowflake_conn):
        """
            :param s3_path: The path used to access S3 directories
            :param snowflake_conn: The connection method used to connect to Snowflake: str ('standard', 'spark')
        """
        self.endpoint = endpoint
        curr_date = dt.date.today()
        year = curr_date.year

        # Build endpoint URL
        if source == 'season':
            self.url = f"{self.endpoint}NHL_{year}_games.html#games"
            self.filename = f"NHL_{year}_regular_season"
            # Build S3 URI to correct path location
            # s3_path = s3_path + '/season/'
        elif source == 'playoffs':
            self.url = f"{self.endpoint}NHL_{year}_games.html#games_playoffs"
            self.filename = f"NHL_{year}_playoff_season"
            # Build S3 URI to correct path location
            # s3_path = s3_path + '/playoffs/'
        elif source == 'stats':
            self.url = f"{self.endpoint}NHL_{year}_games.html#stats"
            self.filename = f"NHL_{year}_team_stats"
            # Build S3 URI to correct path location
            # s3_path = s3_path + '/playoffs/'

        self.s3 = s3_path
        self.conn = get_snowflake_connection(snowflake_conn)

        # Run raw ingestion of data from source
        output_df = self.file_parser(self.url)
        self.s3_parser(output_df)

    @staticmethod
    def file_parser(url: str):
        """ Download raw source data and upload to S3
            Data Source: hockeyreference.com
        """
        try:
            response = get_legacy_session().get(url)
            dataframes = pd.read_html(response.text)

            dataframe = pd.concat(dataframes, axis=0, ignore_index=True)

            logger.info(
                f'Retrieved data with columns: {dataframe.columns}'
                f'\n'
                f'Preview: \n{dataframe.head(3)}'
            )

            return dataframe

        except Exception as e:
            logger.error(f'An error occurred while retrieving raw data: {e}')


    @s3_conn
    def s3_parser(self, data):
        try:
            # Establish connection
            s3_client, s3_resource = boto3.client('s3'), boto3.resource('s3')

            data = data.to_csv()

            # Retrieve S3 paths & store raw file to s3
            logger.info(f'Storing parsed data in S3 at {self.s3}')
            s3_client.put_object(Bucket=self.s3, Key=self.filename, Body=data)
        except Exception as e:
            logger.error(f'An error occurred when storing data in S3: {e}')

    def snowflake_query_exec(self, queries):
        try:
            # Cursor & Connection
            curs, conn = self.conn.cursor(), self.conn
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


if __name__ in "__main__":

    parser = argparse.ArgumentParser(
        prog="SnowflakeIngestion",
        description="Move data from raw S3 uploads to a produced Schema in Snowflake"
    )

    parser.add_argument('source')
    parser.add_argument('endpoint')
    parser.add_argument('s3_path')
    parser.add_argument('snowflake_conn')

    args = parser.parse_args()

    source = args.source if args.source is not None else ""
    endpoint = args.endpoint if args.endpoint is not None else ""
    s3_path = args.s3_path if args.s3_path is not None else ""
    snowflake_conn = args.snowflake_conn if args.snowflake_conn is not None else ""

    execute = SnowflakeIngest(source, endpoint, s3_path, snowflake_conn)

    # CREATE SCHEMA
    schema = execute.snowflake_query_exec(snowflake_schema_queries)

    # INGEST RAW DATA
    # execute.snowflake_query_exec(snowflake_raw_ingest)
