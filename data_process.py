import pandas as pd




def lap_process(df: pd.DataFrame):
    df = df[df['date_start'].notna()]
    return df

def driver_process(df: pd.DataFrame):
    #there is for some reason missing country code in newer data just not using now
    processed_df = df.dropna(subset=['team_name'])
    processed_df = processed_df.dropna(subset=['country_code'])
    return processed_df