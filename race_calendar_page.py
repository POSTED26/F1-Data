import streamlit as st

#my modules
import data_extracter


st.sidebar.markdown('Race Calendar')
st.write('Shows completed races for given F1 season')

year_select = [2023,2024,2025]
selected_year = st.selectbox('select year',year_select, index=len(year_select)-1)
race_list = data_extracter.get_race_list(selected_year)
race_list