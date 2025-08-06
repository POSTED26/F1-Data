-- [['meeting_key', 'session_key', 'session_name', 'session_type', 'country_name', 'year']]

CREATE TABLE IF NOT EXISTS sessions (
    meeting_key Integer,
    session_key Integer,
    session_name TEXT,
    session_type TEXT,
    country_name TEXT,
    year Integer,
    PRIMARY KEY(session_key)
);