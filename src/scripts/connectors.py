import sys
import boto3
import botocore.exceptions

from logger import logger
# from secrets_access import get_secret
import snowflake.connector
from snowflake.snowpark import Session
import os
from dotenv import load_dotenv, find_dotenv

# Get environment vars
load_dotenv(find_dotenv())

# Set logger
logger = logger('connector')


def get_snowflake_connection(method):
    """ Confirm Access to Snowflake"""
    try:
        params = {
            "user": os.getenv('USER'),
            "password": os.getenv('SFPW'),
            "account": os.getenv('ACCT'),
            "warehouse": os.getenv('WAREHOUSE'),
            "database": os.getenv('DB'),
            "schema": os.getenv('SCHEMA')
        }

        if method == 'standard':
            conn = snowflake.connector.connect(
                user=params['user'],
                password=params['password'],
                account=params['account'],
                warehouse=params['warehouse'],
                database=params['database'],
                schema=params['schema']
            )

            return conn

        if method == 'spark':
            session = Session.builder.configs(params).create()
            return session

    except Exception as e:
        raise e


def s3_conn(func):
    """ Confirm Access to S3 """
    def wrapper_s3_checks(*args, **kwargs):
        """ Provides a series of checks for S3 before running a function provided """

        s3_client, s3_resource = boto3.client('s3'), boto3.resource('s3')
        logger.info('Successfully connected to S3.')

        # Stores the passed args and kwargs into available lists
        arg_vars, kw_vars = [arg for arg in args], [arg for arg in kwargs]

        # CHECK ONE
        # Check if the bucket exists. If a bucket exists, it should be passed into a function as an argument.
        buckets = [name['Name'] for name in s3_client.list_buckets()['Buckets']]
        matches = [name for name in arg_vars if name in buckets]
        if not matches:
            logger.error(f'S3 Bucket provided does not exist: {arg_vars}')

        return func(*args, **kwargs)

    return wrapper_s3_checks





