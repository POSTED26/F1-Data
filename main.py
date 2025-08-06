import pandas as pd
from urllib.request import urlopen
import json
import matplotlib.pyplot as plt
import streamlit as st


import data_extracter
import data_process
import data_loader
import db_connector



"""
    thinsgs to do
    - remove streamlit pages and other things
    - figure out what to do with my tables (each have a table and cummulitive table)
    - clean data
    - put into tables
    - maybe put into docker container


"""

def main():

    

    # extract data from api
    #df = data_extracter.get_race_list()
    #print(df)
    #df = data_extracter.get_sessions_year_list('Race')

    meeting_df = data_extracter.get_race_list()
    sessions_df = data_extracter.get_sessions_year_list('Race')
    driver_df = data_extracter.get_drivers()
    pos_df = data_extracter.get_session_result()

    # transform



    driver_cummulative_df = driver_df.merge(sessions_df, how='inner', on=['session_key', 'meeting_key'])
    driver_cummulative_df = driver_cummulative_df.merge(pos_df, how='inner', on=['session_key', 'driver_number'])
    driver_cummulative_df = driver_cummulative_df.merge(meeting_df, how='inner', on=['meeting_key', 'country_name', 'year'])
    #print(driver_cummulative_df.columns)
    #merge in meeting table as well for meeting name for cumm driver table
    #figure out to get one driver per row
    #print(driver_cummulative_df.sort_values(by=['meeting_key', 'position']).head(40).reset_index(drop='True'))

    # load
    db = db_connector.DbConnector()
    loader = data_loader.DbLoader()
    db.connect()
    #create tables
    loader.create_table('meetings.sql', db)
    loader.create_table('sessions.sql', db)
    loader.create_table('drivers.sql', db)

    # load the tables
    # TODO: figure out how to load without sql alchemy or just use it
    loader.load_table(meeting_df, 'meetings', db)
    loader.load_table(sessions_df, 'sessions', db)
    loader.load_table(driver_df, 'drivers', db)

    db.disconnect()
    '''
    sdf = data_loader.get_session(9617)
    print(sdf)
    #9617 2024 us gp race session
    sessdf = data_loader.get_session_result(9617,3)
    print(sessdf)
    drivdf = data_loader.get_driver(1, 9617)
    print(drivdf)
    drvdf = data_loader.get_drivers(9617)
    topdf = sessdf.merge(drvdf, on='driver_number', how='left')
    print(topdf)
    lapdf = data_extracter.get_laps(9617)
    processed_lap = data_process.lap_process(lapdf)
    yuki_laps = processed_lap[processed_lap['driver_number'] == 20].copy()
    yuki_laps = yuki_laps[['lap_duration', 'lap_number']]
    yuki_plot = yuki_laps.plot(kind="line", x='lap_number', y='lap_duration', ylabel='Lap times in seconds', title='Yuki lap times US GP')
    
    posdf = data_extracter.get_position(9617)
    posdf = posdf[posdf['meeting_key'] == 1247].copy()
    posdf = posdf[posdf['driver_number'] == 20].copy()
    yuki_pos = posdf.plot(kind='line', x='date', y='position', title='yuki position US GP')
    #pos =pd.merge(processed_lap, posdf, left_on=['meeting_key','date_start', 'driver_number'], right_on=['meeting_key','date','driver_number'], how='inner').sort_values('driver_number', ascending=True)
    #print(posdf.sort_values(''))
    #print(processed_lap.head(10))
    #print()
    #plt.show()
    '''




if __name__ == "__main__":
    main()


