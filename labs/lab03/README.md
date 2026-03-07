# Lab 03: The Case of the Golden Figurine

A SQL detective mystery where you use PostgreSQL to find the thief who stole a golden figurine during DataCon.

## Prerequisites

- Docker and Docker Compose installed

## Quick Start

### 1. Start the database

```bash
docker-compose up -d
```

Wait a few seconds for PostgreSQL to initialize.

### 2. Load the data

```bash
docker exec -i local_pgdb psql -U hsuadmin -d postgres < setup_and_solve.sql
```

Expected output:
```
CREATE TABLE (x4)
COPY 4
COPY 40
COPY 49
COPY 43
```

### 3. Run the solution query

```bash
docker exec -i local_pgdb psql -U hsuadmin -d postgres < solution.sql
```

### 4. Verify the result

The thief's hash should match: `a047207351d4bac07bb6b1a5d944d060`

## Cleanup

```bash
docker-compose down -v
```

## Database Access

- **PostgreSQL**: localhost:5432 (user: `hsuadmin`, password: `hsu123`)
- **pgAdmin**: http://localhost:8888 (email: `admin@admin.com`, password: `hsu234`)

## Files

| File | Description |
|------|-------------|
| `docker-compose.yml` | PostgreSQL and pgAdmin setup |
| `setup_and_solve.sql` | Creates tables and loads CSV data |
| `solution.sql` | Detective query to find the thief |
| `data/` | CSV files (people, sessions, swipes, purchases) |
