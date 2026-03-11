-- Create the fuel transactions table
CREATE TABLE IF NOT EXISTS fuel_transactions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(100) UNIQUE NOT NULL,
    station_id INTEGER NOT NULL,
    dock_bay SMALLINT,
    dock_level VARCHAR(10),
    ship_name VARCHAR(255),
    franchise VARCHAR(255),
    captain_name VARCHAR(255),
    species VARCHAR(100),
    fuel_type VARCHAR(100),
    fuel_units REAL,
    price_per_unit DECIMAL(8, 2),
    total_cost DECIMAL(12, 2),
    services TEXT[],
    is_emergency BOOLEAN DEFAULT FALSE,
    visited_at TIMESTAMP WITH TIME ZONE,
    arrival_date DATE,
    coords_x DOUBLE PRECISION,
    coords_y DOUBLE PRECISION,
    source_file VARCHAR(255),
    loaded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_fuel_transactions_station_id ON fuel_transactions(station_id);
CREATE INDEX IF NOT EXISTS idx_fuel_transactions_visited_at ON fuel_transactions(visited_at);
CREATE INDEX IF NOT EXISTS idx_fuel_transactions_source_file ON fuel_transactions(source_file);

-- Create a table to track processed files
CREATE TABLE IF NOT EXISTS processed_files (
    id SERIAL PRIMARY KEY,
    filename VARCHAR(255) UNIQUE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    rows_loaded INTEGER
);
