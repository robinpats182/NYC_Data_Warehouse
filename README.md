# NYC FHVHV Trips — Mini Data Platform

A end-to-end data engineering pipeline built with PostgreSQL and Python, processing ~20.9 million NYC High-Volume For-Hire Vehicle (FHVHV) trip records for January 2026.

## Architecture

```
Parquet File (505 MB)
       ↓
  Python Ingestion (pyarrow + SQLAlchemy)
       ↓
staging.fhv_trip_raw        ← Raw load, 20.9M rows, no transformation
       ↓
  SQL Transformation (03_transform_modeled.sql)
       ↓
modeled.fhv_trip_clean      ← Typed, cleaned, quality-filtered
       ↓
  SQL Star Schema Build (04_build_analytics_star.sql)
       ↓
analytics.fact_trip + dimensions  ← Star schema for BI queries
       ↓
  SQL Views (05_analytics_views.sql)
       ↓
6 BI Views (Q1–Q6)          ← Ready for analysis
```

## Project Structure

```
NYC/
├── src/
│   ├── ingest_fhv.py           # Chunked batch ingestion pipeline
│   ├── load_zones.py           # Taxi zone lookup loader
│   └── data/                   # Place downloaded data files here (git-ignored)
│       ├── fhvhv_tripdata_2026-01.parquet
│       └── taxi_zone_lookup.csv
├── sql/
│   ├── 01_create_schema.sql    # DDL for all schemas and tables
│   ├── 02_create_staging_tables.sql  # Staging layer definitions
│   ├── 03_transform_modeled.sql      # Staging → Modeled transformation
│   ├── 04_build_analytics_star.sql   # Modeled → Star schema population
│   ├── 05_analytics_views.sql        # Six BI views (Q1–Q6)
│   └── 06_performance.sql            # EXPLAIN plans + index scripts
├── docs/
├── docker-compose.yml
├── requirements.txt
├── .env                        # Your credentials (git-ignored)
└── .gitignore
```

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Python 3.10+
- pgAdmin 4 (bundled with Docker Compose)

## Step-by-Step Setup

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/nyc-fhvhv-platform.git
cd nyc-fhvhv-platform
```

### 2. Download the Data

**Trip data:**
1. Go to [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
2. Download `fhvhv_tripdata_2026-01.parquet`
3. Save to `src/data/fhvhv_tripdata_2026-01.parquet`

**Zone lookup:**
1. On the same page, download `Taxi Zone Lookup Table (CSV)`
2. Save to `src/data/taxi_zone_lookup.csv`

### 3. Configure Environment Variables

Create a `.env` file in the project root:

```
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=nyc_fhv
DB_USER=nyc_user
DB_PASS=nyc_pass
```

### 4. Start Docker

```bash
docker compose up -d
```

This starts:
- **PostgreSQL 15** on port `5432`
- **pgAdmin 4** on [localhost:8080](http://localhost:8080)

pgAdmin login: `admin@admin.com` / `admin`

pgAdmin server connection:
- Host: `postgres`
- Port: `5432`
- Database: `nyc_fhv`
- Username: `nyc_user`
- Password: `nyc_pass`

### 5. Install Python Dependencies

```bash
python -m venv .venv
source .venv/Scripts/activate  # Windows
pip install -r requirements.txt
```

### 6. Create Database Schema

Open pgAdmin at [localhost:8080](http://localhost:8080), open the Query Tool, and run:

```
sql/01_create_schema.sql
```

This creates all schemas (`staging`, `modeled`, `analytics`) and all tables.

### 7. Run Batch Ingestion

```bash
python src/ingest_fhv.py
```

- Reads the parquet file in batches of 50,000 rows using pyarrow
- Applies quality filters (null checks, non-negative values)
- Loads ~20.9M rows into `staging.fhv_trip_raw`
- Logs run metadata to `staging.etl_run_log`

> ⚠️ This will take several hours due to the size of the dataset.

Verify ingestion:
```bash
docker exec -it nyc_postgres psql -U nyc_user -d nyc_fhv -c "SELECT rows_read, rows_loaded, rejected_rows, status FROM staging.etl_run_log ORDER BY run_id DESC LIMIT 1;"
```

### 8. Load Zone Lookup

```bash
python src/load_zones.py
```

Loads 265 NYC taxi zones into `analytics.dim_zone`.

### 9. Run SQL Transformations (in order via pgAdmin Query Tool)

**Step 1 — Transform to modeled layer** (~15-20 min):
```
sql/03_transform_modeled.sql
```
Cleans and types the staging data into `modeled.fhv_trip_clean`.

**Step 2 — Build star schema** (~40 min):
```
sql/04_build_analytics_star.sql
```
Populates `dim_date`, `dim_time`, `dim_provider`, and `analytics.fact_trip`.

**Step 3 — Create BI views** (instant):
```
sql/05_analytics_views.sql
```
Creates all six analytics views.

**Step 4 — Add indexes and run EXPLAIN** (optional, for performance analysis):
```
sql/06_performance.sql
```
Run STEP 1 (EXPLAIN before indexes), then STEP 2 (create indexes), then STEP 3 (EXPLAIN after indexes).

## Business Intelligence Views

| View | Question |
|------|----------|
| `analytics.v_daily_demand` | Q1 — Trips per calendar day + demand rank |
| `analytics.v_top5_busiest_days` | Q1 — Top 5 busiest days |
| `analytics.v_peak_hours` | Q2 — Trips per hour, weekday vs weekend |
| `analytics.v_service_responsiveness` | Q3 — Avg wait time by hour of day |
| `analytics.v_trip_efficiency` | Q4 — Avg speed proxy + unrealistic value flags |
| `analytics.v_revenue_components` | Q5 — Total fare, tips, tips as % of fare by day |
| `analytics.v_top_pickup_zones` | Q6 — Top 10 pickup zones by trip count |
| `analytics.v_top_dropoff_zones` | Q6 — Top 10 dropoff zones by trip count |
| `analytics.v_location_hotspots` | Q6 — Top 10 pickup/dropoff zone pairs |

Query any view directly:
```sql
SELECT * FROM analytics.v_top5_busiest_days;
SELECT * FROM analytics.v_peak_hours;
SELECT * FROM analytics.v_location_hotspots;
```

## Data Model

### Star Schema

```
              dim_date
                 |
dim_provider — fact_trip — dim_zone (pickup)
                 |              |
              dim_time      dim_zone (dropoff)
```

| Table | Rows | Description |
|-------|------|-------------|
| `staging.fhv_trip_raw` | ~20.9M | Raw parquet load |
| `modeled.fhv_trip_clean` | ~20.9M | Cleaned, typed data |
| `analytics.fact_trip` | ~20.9M | Star schema fact table |
| `analytics.dim_date` | 31 | Calendar attributes |
| `analytics.dim_time` | 24 | Hour of day attributes |
| `analytics.dim_zone` | 265 | NYC taxi zones + boroughs |
| `analytics.dim_provider` | 4 | Uber, Lyft, Juno, Via |

## Performance

Indexes added on `fact_trip` for BI query performance:

| Index | Column | Benefit |
|-------|--------|---------|
| `idx_fact_date_id` | `date_id` | Q1, Q2, Q5 daily aggregations |
| `idx_fact_time_id` | `time_id` | Q2, Q3, Q4 hourly aggregations |
| `idx_fact_pu_location` | `pu_location_id` | Q6 zone hot-spots |
| `idx_fact_do_location` | `do_location_id` | Q6 zone hot-spots |
| `idx_fact_pickup_dt` | `pickup_datetime` | Range queries |

**Before indexes:** ~2 min per BI query
**After indexes:** ~11-12 sec per BI query (~10x improvement)

## Stopping and Restarting

```bash
# Stop (data is preserved)
docker compose down

# Restart
docker compose up -d
```

> ⚠️ Never run `docker compose down -v` — this deletes all data volumes.

## Dataset

- **Source:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Dataset:** FHVHV January 2026
- **File size:** ~505 MB (parquet)
- **Rows:** 20,940,373
- **Providers:** Uber (HV0003), Lyft (HV0005), Juno (HV0002), Via (HV0004)