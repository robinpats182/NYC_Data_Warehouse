import pyarrow.dataset as ds
from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
PARQUET_FILE = BASE_DIR / "src" / "data" / "fhvhv_tripdata_2026-01.parquet"

load_dotenv()

# Database connection
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

# Custom insert method using executemany
def pg_insert(table, conn, keys, data_iter):
    rows = [dict(zip(keys, row)) for row in data_iter]
    placeholders = ", ".join([f":{k}" for k in keys])
    sql = f"INSERT INTO {table.schema}.{table.name} ({', '.join(keys)}) VALUES ({placeholders})"
    conn.execute(text(sql), rows)

dataset = ds.dataset(PARQUET_FILE, format="parquet")

start_time = datetime.now()
rows_read = 0
rows_loaded = 0

with engine.begin() as conn:
    conn.execute(text("""
        INSERT INTO staging.etl_run_log
        (dataset, start_ts, status)
        VALUES ('fhvhv_tripdata_2026-01.parquet', NOW(), 'RUNNING')
    """))

for batch in dataset.to_batches(batch_size=50000):
    df = batch.to_pandas()
    rows_read += len(df)

    # Basic quality filters
    df = df[
        (df["pickup_datetime"].notnull()) &
        (df["dropoff_datetime"].notnull()) &
        (df["trip_miles"] >= 0) &
        (df["trip_time"] >= 0)
    ]

    rows_loaded += len(df)

    df.to_sql(
        "fhv_trip_raw",
        engine,
        schema="staging",
        if_exists="append",
        index=False,
        method=pg_insert,
        chunksize=1000
    )

end_time = datetime.now()

with engine.begin() as conn:
    conn.execute(text("""
        UPDATE staging.etl_run_log
        SET end_ts = NOW(),
            rows_read = :rows_read,
            rows_loaded = :rows_loaded,
            rejected_rows = :rejected,
            status = 'SUCCESS'
        WHERE run_id = (SELECT MAX(run_id) FROM staging.etl_run_log)
    """), {
        "rows_read": rows_read,
        "rows_loaded": rows_loaded,
        "rejected": rows_read - rows_loaded
    })

print(f"Ingestion completed. Read: {rows_read:,} | Loaded: {rows_loaded:,} | Rejected: {rows_read - rows_loaded:,}")