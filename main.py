import pandas as pd
from urllib.request import urlopen
import json
import matplotlib.pyplot as plt
import streamlit as st

import data_extracter
import data_process

"""
    thinsgs to do
    - remove streamlit pages and other things
    - figure out what to do with my tables (each have a table and cummulitive table)
    - clean data
    - put into tables
    - maybe put into docker container


"""

def main():
    '''df = data_loader.get_race_list(2024)
    print(df)
    df = data_loader.get_sessions_year_list('Race', 2024)
    print(df)
    sdf = data_loader.get_session(9617)
    print(sdf)
    #9617 2024 us gp race session
    sessdf = data_loader.get_session_result(9617,3)
    print(sessdf)
    drivdf = data_loader.get_driver(1, 9617)
    print(drivdf)
    drvdf = data_loader.get_drivers(9617)
    topdf = sessdf.merge(drvdf, on='driver_number', how='left')
    print(topdf)'''
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
    plt.show()

    # try streamlit to show rece list for each year.
    '''st.set_page_config('F1 Dashboard')
    st.title('F1 Data Dashboard')
    st.write('F1 data dashboard landing page. Navigate to different pages via the sidebar to explore F1 data!')

    main_page = st.Page("main.py", title='Home')
    race_calendar_page = st.Page("race_calendar_page.py", title='Race Calendar')

    pg = st.navigation([main_page, race_calendar_page])
    pg.run()'''
    '''year_select = [2023,2024,2025]
    selected_year = st.selectbox('select year',year_select, index=len(year_select)-1)
    race_list = data_loader.get_race_list(selected_year)
    race_list'''


if __name__ == "__main__":
    main()


