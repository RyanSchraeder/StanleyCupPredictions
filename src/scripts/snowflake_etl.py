import pandas as pd
import datetime as dt
import os
import boto3
from secrets_access import get_secret
from typing import List
## Data Source: https://www.hockey-reference.com/leagues/NHL_2022.html ##

path = '/Users/P2945649/Documents/Projects/StanleyCupPredictions/data/'
secret = get_secret()


# TODO: Replace this function with access to the files direct from source, whether that be Hockey Reference or NHL API.
# TODO: The files will need to be ingested, processed, transformed, then loaded into S3, which is accessed via Snowflake.
# TODO: The Snowflake script for this data is saved in the utils, and has stages assigned per S3 bucket.

def file_access(path: str):
    dataframes = []
    filenames = []
    for root, dirs, files in os.walk(path, topdown=False):
        for name in files:
            filenames.append(name)
            df = pd.read_csv(os.path.join(root, name))
            dataframes.append(df)

    return dataframes

    # # CONFIRM ACCESS TO S3
    # s3_client = boto3.client('s3')
    # response = s3_client.list_buckets()
    # return response


team_stats_df = file_access(path)[0]
team_stats_updated = file_access(path)[1]
games_df = file_access(path)[2]

games_df = games_df.rename(columns=({
    'Date': 'date', 'Visitor': 'away_team', 'Home': 'home_team', 'G': 'away_goals', 'G.1': 'home_goals', 'LOG': 'length_of_game_min'
}))
games_df = games_df[['date', 'away_team', 'away_goals', 'home_team', 'home_goals', 'length_of_game_min']]

# Transforming data
games_df['length_of_game_min'] = [i.replace(':', '') for i in games_df['length_of_game_min']]
games_df['length_of_game_min'] = [(int(i[0]) * 60) + int(i[1:]) for i in games_df['length_of_game_min']]

games_df.date = games_df.date.apply(pd.to_datetime)


def encoding_game_outcome(dataset, away_output_colname: str, home_output_colname: str, away_goals: str,
                          home_goals: str) -> List[int]:
    dataset[f'{away_output_colname}'] = (dataset[f'{away_goals}'] - dataset[f'{home_goals}']).apply(
        lambda x: 1 if x > 0 else 0)
    dataset[f'{home_output_colname}'] = (dataset[f'{home_goals}'] - dataset[f'{away_goals}']).apply(
        lambda x: 1 if x > 0 else 0)

    return dataset


games_df = encoding_game_outcome(games_df, 'away_outcome', 'home_outcome', 'away_goals', 'home_goals')

# Output to CSV
games_df.to_csv(os.path.join(path, 'regular_season_clean.csv'), index=False)

# Team name cleaning
team_stats_df['Team'] = [str(i).replace('*', '') for i in team_stats_df['Team']]

# Creating Column for Total Goals

team_stats_df['G'] = team_stats_df.GF + team_stats_df.GA

# Creating Column for Total Power-Play Goals

team_stats_df['PPG'] = team_stats_df.PP + team_stats_df.PPA

# Creating Column for Total Games in Shootouts

team_stats_df['SHOOTOUTS'] = team_stats_df.SOW + team_stats_df.SOL


def percents(df):
    for column, row in df.iteritems():
        if '%' in column:
            for item in row:
                if item < 1:
                    row += row * 100

    return df


team_stats_df = percents(team_stats_df)

# Output to CSV
team_stats_df.to_csv(os.path.join(path, 'team_stats_clean.csv'), index=False)