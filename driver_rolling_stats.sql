-- create from other tables


CREATE TABLE IF NOT EXISTS driver_rolling_stats (
    driver_number Integer,
    full_name TEXT,
    country_code TEXT,
    team_name TEXT,
    meeting_key Integer,
    session_key Integer,
    session_name TEXT,
    session_type TEXT,
    country_name TEXT,
    year Integer,
    position Integer,
    meeting_name TEXT,
    PRIMARY KEY(full_name, country_code, meeting_key, session_key)
);