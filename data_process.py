import pandas as pd

"""
-maybe try star schema
-cummulative diver dim (diver_name, other non changing things, {season_year, points, team, etc.})
-probably load into their own tables after cleaning then on dbeaver make cummulitive table. 
-figure out how to run and sql file from python code to be able to create the cummulitive table

tables for session, meeting, driver and a cummulitive driver dim table

TODO:look into null values in other tables
"""


def lap_process(df: pd.DataFrame):
    df = df[df['date_start'].notna()]
    return df

def driver_process(df: pd.DataFrame):
    processed_df = df.dropna(subset=['country_code'])
    return processed_df