import pandas as pd
from urllib.request import urlopen



import data_extracter
import data_process
import data_loader
import db_connector





def main():

    

    # extract data from api
    meeting_df = data_extracter.get_race_list()
    sessions_df = data_extracter.get_sessions_year_list('Race')
    driver_df = data_extracter.get_drivers()
    pos_df = data_extracter.get_session_result()


    # transform

    driver_df = data_process.driver_process(driver_df)

    driver_cummulative_df = driver_df.merge(sessions_df, how='inner', on=['session_key', 'meeting_key'])
    driver_cummulative_df = driver_cummulative_df.merge(pos_df, how='inner', on=['session_key', 'driver_number'])
    driver_cummulative_df = driver_cummulative_df.merge(meeting_df, how='inner', on=['meeting_key', 'country_name', 'year'])

    
    # load
    db = db_connector.DbConnector()
    loader = data_loader.DbLoader()
    db.connect()
    #create tables
    loader.create_table('meetings.sql', db)
    loader.create_table('sessions.sql', db)
    loader.create_table('drivers.sql', db)
    loader.create_table('session_results.sql', db)
    loader.create_table('driver_rolling_stats.sql', db)
    
    # load the tables
    loader.load_table(meeting_df, 'meetings', db)
    loader.load_table(sessions_df, 'sessions', db)
    loader.load_table(driver_df, 'drivers', db)
    loader.load_table(pos_df, 'session_results', db)
    loader.load_table(driver_cummulative_df, 'driver_rolling_stats', db)
    




if __name__ == "__main__":
    main()


