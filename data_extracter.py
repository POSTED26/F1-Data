import pandas as pd
import requests
from pyspark.sql import SparkSession

from dotenv import load_dotenv
import os

'''
    TODO: finish moving to pyspark dataframes
'''

load_dotenv()

BASE_URL = os.getenv('BASE_API_URL') #'https://api.openf1.org/v1/'

def get_data(endpoint, params=None):
    """
    Get data from F1 open api

    Args:
        endpoint (str): endpoint you want to pull data from (meeting, session, driver, etc.)
        params (dict): optional, drill down more specifics for getting data

    Rreturn:
        spark dataframe: returns a data frame for easy clean and manipulation 
        spark session: returns spark session for further use
    """
    if params is None:
        params = {}

    spark = SparkSession.builder.appName("pull_data").getOrCreate()

    url = f"{BASE_URL}{endpoint}"
    full_url = requests.Request('GET', url, params=params).prepare().url
    response = requests.get(full_url)
    response.raise_for_status()
    data = response.json()
    spark = SparkSession.builder.appName("pull_data").getOrCreate()
    df = spark.createDataFrame(data) 
    return df, spark


# use get_data to pull what we want

def get_race_list():
    """
    Args:
        year (int): F1 season year
    
    Return:
        spark DataFrame: returns data fram of all races in year provided
    """
    spark_df, spark = get_data("meetings")

    

    if spark_df.isEmpty():
        print('No data for meetings selected')
        return spark.createDataFrame([])
    

    
    return spark_df.select('meeting_key','meeting_name','country_name', 'year')


def get_sessions_year_list(session_type):

    df = get_data('sessions', {'session_type': session_type})

    if df.empty:
        print('No data for sessions list you selected')
        return pd.DataFrame()
    
    return df[['meeting_key', 'session_key', 'session_name', 'session_type', 'country_name', 'year']]


    
def get_session(session_key):
    
    df = get_data('sessions', {'session_key': session_key})

    if df.empty:
        print('No data for session')
        return pd.DataFrame()
    
    return df[['session_key', 'session_name', 'session_type', 'country_name', 'year']]
    
def get_session_result(num_of_pos=20):

    df = get_data('session_result', {'position<':num_of_pos})
    if df.empty:
        print('No date for session result')
        return pd.DataFrame()
    df = df.sort_values(by=['position'])
    return df[['session_key', 'driver_number', 'position']]

def get_driver(driver_number, session_key):
    """
        Get info on a single driver from a session
    """
    df = get_data('drivers', {'driver_number': driver_number, 'session_key': session_key})
    if df.empty:
        print('No driver data for selected number')
        return pd.DataFrame()
    return df[['driver_number', 'full_name', 'team_name']]

def get_drivers():
    """
        Get list of drivers from a session
    """
    df = get_data('drivers')
    if df.empty:
        print('No driver data for selected number')
        return pd.DataFrame()
    return df[['driver_number', 'full_name', 'country_code', 'team_name', 'meeting_key', 'session_key']]

def get_laps(session_key):
    """
        Get lap information for all drivers in a session
    """
    df = get_data("Laps", {'session_key': session_key})
    if df.empty:
        print('No lap data for given session')
        return pd.DataFrame()
    return df[['meeting_key','session_key', 'date_start', 'driver_number', 'lap_duration', 'lap_number']]

def get_position(session_key):
    """
        Get position data for a session
    """
    df = get_data('position', {'session_key':session_key})
    if df.empty:
        print('No position data for session')
        return pd.DataFrame()
    return df[['meeting_key','session_key', 'date', 'driver_number', 'position']]