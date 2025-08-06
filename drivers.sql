-- [['driver_number', 'full_name', 'country_code', 'team_name', 'meeting_key', 'session_key']]
CREATE TABLE IF NOT EXISTS drivers (
    driver_number Integer,
    full_name TEXT,
    country_code TEXT,
    team_name TEXT,
    meeting_key Integer,
    session_key Integer,
    PRIMARY KEY(full_name, country_code)
);