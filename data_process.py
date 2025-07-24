import pandas as pd




def lap_process(df: pd.DataFrame):
    df = df[df['date_start'].notna()]
    return df