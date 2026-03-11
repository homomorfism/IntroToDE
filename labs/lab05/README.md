# Lab 05: ETL via Airflow

This lab implements an ETL pipeline using Apache Airflow to process Parquet files containing synthetic fuel station transaction data and load them into PostgreSQL.

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Data Generator │────▶│   data/ folder  │────▶│   Airflow DAG   │
│  (gen-data.py)  │     │  (Parquet files)│     │  (every minute) │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
                                                         ▼
                                               ┌─────────────────┐
                                               │   PostgreSQL    │
                                               │ (fuel_station)  │
                                               └─────────────────┘
```

## Prerequisites

- Docker and Docker Compose
- Python 3.9+ (for running the data generator locally)

## Quick Start

### 1. Start the services

```bash
# Set the Airflow UID (required on Linux)
echo "AIRFLOW_UID=$(id -u)" > .env

# Start all services
docker compose up -d
```

### 2. Wait for Airflow to initialize

It takes about 1-2 minutes for Airflow to fully initialize. You can check the logs:

```bash
docker compose logs -f airflow-init
```

### 3. Access Airflow UI

Open http://localhost:8080 in your browser.

- **Username:** admin
- **Password:** admin

### 4. Run the data generator

In a separate terminal:

```bash
# Install dependencies
pip install faker pyarrow

# Run the generator
python gen-data.py --rows-per-file 300 --period-seconds 60
```

### 5. Monitor the ETL

- The DAG `fuel_station_etl` runs every minute
- Check the Airflow UI to see task runs
- Processed files are moved to the `processed/` directory

## Accessing PostgreSQL

The target PostgreSQL database is exposed on port 5433:

```bash
# Connect using psql
psql -h localhost -p 5433 -U etl_user -d fuel_station

# Or use docker exec
docker compose exec postgres-target psql -U etl_user -d fuel_station
```

### Useful queries

```sql
-- Count total records
SELECT COUNT(*) FROM fuel_transactions;

-- View recent transactions
SELECT transaction_id, ship_name, franchise, fuel_type, total_cost, visited_at
FROM fuel_transactions
ORDER BY visited_at DESC
LIMIT 10;

-- Check processed files
SELECT * FROM processed_files ORDER BY processed_at DESC;

-- Aggregations by franchise
SELECT franchise, COUNT(*) as transactions, SUM(total_cost) as total_revenue
FROM fuel_transactions
GROUP BY franchise
ORDER BY total_revenue DESC;
```

## Project Structure

```
lab05/
├── docker-compose.yml    # Docker Compose configuration
├── gen-data.py           # Data generator script
├── requirements.txt      # Python dependencies
├── README.md             # This file
├── dags/
│   └── fuel_etl_dag.py   # Airflow DAG definition
├── init-db/
│   └── 01_create_tables.sql  # PostgreSQL schema
├── data/                 # Input: new Parquet files
├── processed/            # Output: processed Parquet files
└── logs/                 # Airflow logs
```

## DAG Details

The `fuel_station_etl` DAG consists of three tasks:

1. **check_for_new_files**: Scans the `data/` directory for new Parquet files that haven't been processed yet
2. **process_and_load_data**: Reads each new file, transforms the data, and inserts it into PostgreSQL
3. **move_processed_files**: Moves successfully processed files to the `processed/` directory

## Stopping the Services

```bash
docker compose down

# To also remove volumes (databases)
docker compose down -v
```

## Troubleshooting

### DAG not appearing in Airflow UI

Wait a few minutes for Airflow to scan the dags folder, or restart the scheduler:

```bash
docker compose restart airflow-scheduler
```

### Permission issues on Linux

Make sure to set the AIRFLOW_UID:

```bash
echo "AIRFLOW_UID=$(id -u)" > .env
docker compose down && docker compose up -d
```

### View DAG logs

```bash
docker compose logs airflow-scheduler
```
