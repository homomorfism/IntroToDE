-- Create tables
CREATE TABLE IF NOT EXISTS sessions (
    session_code VARCHAR(10) PRIMARY KEY,
    session_name VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS people (
    person_id INTEGER PRIMARY KEY,
    full_name VARCHAR(100),
    team VARCHAR(50),
    badge_uid VARCHAR(50) UNIQUE
);

CREATE TABLE IF NOT EXISTS swipes (
    badge_uid VARCHAR(50),
    session_code VARCHAR(10),
    ts TIMESTAMP
);

CREATE TABLE IF NOT EXISTS purchases (
    purchase_id INTEGER PRIMARY KEY,
    badge_uid VARCHAR(50),
    location VARCHAR(50),
    product VARCHAR(50),
    qty INTEGER,
    ts TIMESTAMP
);

-- Load data from CSV files
COPY sessions(session_code, session_name) 
FROM '/data/sessions.csv' 
DELIMITER ',' CSV HEADER;

COPY people(person_id, full_name, team, badge_uid) 
FROM '/data/people.csv' 
DELIMITER ',' CSV HEADER;

COPY swipes(badge_uid, session_code, ts) 
FROM '/data/swipes.csv' 
DELIMITER ',' CSV HEADER;

COPY purchases(purchase_id, badge_uid, location, product, qty, ts) 
FROM '/data/purchases.csv' 
DELIMITER ',' CSV HEADER;

-- Verify data loaded
SELECT 'people' as table_name, COUNT(*) as row_count FROM people
UNION ALL
SELECT 'sessions', COUNT(*) FROM sessions
UNION ALL
SELECT 'swipes', COUNT(*) FROM swipes
UNION ALL
SELECT 'purchases', COUNT(*) FROM purchases;
