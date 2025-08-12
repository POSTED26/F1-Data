-- [['session_key', 'driver_number', 'position']]
CREATE TABLE IF NOT EXISTS session_results (
    session_key Integer,
    driver_number Integer,
    position Integer,
    PRIMARY KEY(session_key, driver_number)
);