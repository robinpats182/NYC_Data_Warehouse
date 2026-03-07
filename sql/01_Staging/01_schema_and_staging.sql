-- Staging layer table definitions

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS modeled;
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS staging.fhv_trip_raw (
    hvfhs_license_num       TEXT,
    dispatching_base_num    TEXT,
    originating_base_num    TEXT,
    request_datetime        TIMESTAMP,
    on_scene_datetime       TIMESTAMP,
    pickup_datetime         TIMESTAMP,
    dropoff_datetime        TIMESTAMP,
    PULocationID            INTEGER,
    DOLocationID            INTEGER,
    trip_miles              DOUBLE PRECISION,
    trip_time               BIGINT,
    base_passenger_fare     DOUBLE PRECISION,
    tolls                   DOUBLE PRECISION,
    bcf                     DOUBLE PRECISION,
    sales_tax               DOUBLE PRECISION,
    congestion_surcharge    DOUBLE PRECISION,
    airport_fee             DOUBLE PRECISION,
    tips                    DOUBLE PRECISION,
    driver_pay              DOUBLE PRECISION,
    shared_request_flag     TEXT,
    shared_match_flag       TEXT,
    access_a_ride_flag      TEXT,
    wav_request_flag        TEXT,
    wav_match_flag          TEXT,
    cbd_congestion_fee      DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS staging.etl_run_log (
    run_id          SERIAL PRIMARY KEY,
    dataset         TEXT,
    start_ts        TIMESTAMP,
    end_ts          TIMESTAMP,
    rows_read       INTEGER,
    rows_loaded     INTEGER,
    rejected_rows   INTEGER,
    status          TEXT,
    error_message   TEXT
);