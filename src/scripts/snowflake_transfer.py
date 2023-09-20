import pandas as pd
import argparse
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


class SnowflakeIngest(object):

    def __init__(self, s3_path, snowflake_conn):
        """
            :param s3_path: The path used to access S3 directories
            :param snowflake_conn: The connection method used to connect to Snowflake: str ('standard', 'spark')
        """
        self.s3 = s3_path
        self.conn = get_snowflake_connection(snowflake_conn)

    @staticmethod
    def file_parser(local_path):
        """ Use S3 to access files and create dataframes """
        dataframes = []
        filenames = []

        for root, dirs, files in os.walk(local_path, topdown=False):
            logger.info(
                f"""
                root: {root} \n
                dirs: {dirs} \n
                files: {files} \n
                """
            )

            for name in files:
                filenames.append(name)
                df = pd.read_csv(os.path.join(root, name))
                dataframes.append(df)

        return dataframes

    @s3_conn
    def s3_parser(self):

        # Establish connection
        s3_client, s3_resource = boto3.client('s3'), boto3.resource('s3')

        # Retrieve S3 paths
        my_bucket = s3_resource.Bucket(self.s3)
        for my_bucket_object in my_bucket.objects.all():
            logger.info(f'Folder Found: {my_bucket_object.key}')

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

    parser.add_argument('s3_path')
    parser.add_argument('snowflake_conn')

    args = parser.parse_args()

    s3_path = args.s3_path if args.s3_path is not None else ""
    snowflake_conn = args.snowflake_conn if args.snowflake_conn is not None else ""

    execute = SnowflakeIngest(s3_path=s3_path, snowflake_conn=snowflake_conn)

    # CREATE SCHEMA
    schema = execute.snowflake_query_exec(snowflake_schema_queries)

    # INGEST RAW DATA
    execute.snowflake_query_exec(snowflake_raw_ingest)

    # PARSE RAW DATA FROM S3
    s3 = execute.s3_parser()

    # PARSE RAW DATA FROM STORAGE
    #
    # # Paths
    # seasons, team_stats =
    #
    # seasons = file_parser(seasons)
    # team_stats = file_parser(team_stats)
