-- These queries are for documentation purposes only. The actual table creation is done in the postgresql.

-- MODELED LAYER
-- Typed, cleaned, invalid rows removed

CREATE TABLE IF NOT EXISTS modeled.fhv_trip_clean (
    trip_id                 BIGSERIAL PRIMARY KEY,
    hvfhs_license_num       VARCHAR(10)     NOT NULL,
    dispatching_base_num    VARCHAR(10)     NOT NULL,
    originating_base_num    VARCHAR(10),
    request_datetime        TIMESTAMP       NOT NULL,
    on_scene_datetime       TIMESTAMP,
    pickup_datetime         TIMESTAMP       NOT NULL,
    dropoff_datetime        TIMESTAMP       NOT NULL,
    pu_location_id          SMALLINT        NOT NULL,
    do_location_id          SMALLINT        NOT NULL,
    trip_miles              NUMERIC(10,3)   NOT NULL,
    trip_time_seconds       INTEGER         NOT NULL,
    trip_duration_minutes   NUMERIC(10,2)   NOT NULL,
    wait_time_minutes       NUMERIC(10,2),
    base_passenger_fare     NUMERIC(10,2)   NOT NULL,
    tolls                   NUMERIC(10,2)   NOT NULL DEFAULT 0,
    bcf                     NUMERIC(10,2)   NOT NULL DEFAULT 0,
    sales_tax               NUMERIC(10,2)   NOT NULL DEFAULT 0,
    congestion_surcharge    NUMERIC(10,2)   NOT NULL DEFAULT 0,
    airport_fee             NUMERIC(10,2)   NOT NULL DEFAULT 0,
    tips                    NUMERIC(10,2)   NOT NULL DEFAULT 0,
    driver_pay              NUMERIC(10,2)   NOT NULL,
    cbd_congestion_fee      NUMERIC(10,2)   NOT NULL DEFAULT 0,
    total_fare              NUMERIC(10,2)   NOT NULL,
    shared_request_flag     BOOLEAN,
    shared_match_flag       BOOLEAN,
    access_a_ride_flag      BOOLEAN,
    wav_request_flag        BOOLEAN,
    wav_match_flag          BOOLEAN
);

-- DIMENSIONS
-- dim_date
CREATE TABLE IF NOT EXISTS analytics.dim_date (
    date_id         INTEGER         PRIMARY KEY,  -- YYYYMMDD
    full_date       DATE            NOT NULL,
    year            SMALLINT        NOT NULL,
    month           SMALLINT        NOT NULL,
    month_name      VARCHAR(10)     NOT NULL,
    day             SMALLINT        NOT NULL,
    day_of_week     SMALLINT        NOT NULL,  -- 0=Sunday, 6=Saturday
    day_name        VARCHAR(10)     NOT NULL,
    is_weekend      BOOLEAN         NOT NULL,
    week_of_year    SMALLINT        NOT NULL
);

-- dim_time
CREATE TABLE IF NOT EXISTS analytics.dim_time (
    time_id         SMALLINT        PRIMARY KEY,  -- 0-23 (hour of day)
    hour            SMALLINT        NOT NULL,
    time_label      VARCHAR(10)     NOT NULL,  -- e.g. '08:00'
    period_of_day   VARCHAR(20)     NOT NULL,  -- Morning/Afternoon/Evening/Night
    is_peak_hour    BOOLEAN         NOT NULL   -- 7-9am, 4-7pm = peak
);

-- dim_zone
CREATE TABLE IF NOT EXISTS analytics.dim_zone (
    location_id     SMALLINT        PRIMARY KEY,
    zone_code       VARCHAR(10),
    zone_name       VARCHAR(100)    NOT NULL,
    borough         VARCHAR(50)     NOT NULL
);

-- dim_provider
CREATE TABLE IF NOT EXISTS analytics.dim_provider (
    provider_id             SMALLSERIAL     PRIMARY KEY,
    hvfhs_license_num       VARCHAR(10)     NOT NULL UNIQUE,
    provider_name           VARCHAR(50)     NOT NULL
);

-- FACT TABLE
CREATE TABLE IF NOT EXISTS analytics.fact_trip (
    trip_id                 BIGINT          PRIMARY KEY,
    date_id                 INTEGER         NOT NULL REFERENCES analytics.dim_date(date_id),
    time_id                 SMALLINT        NOT NULL REFERENCES analytics.dim_time(time_id),
    pu_location_id          SMALLINT        NOT NULL REFERENCES analytics.dim_zone(location_id),
    do_location_id          SMALLINT        NOT NULL REFERENCES analytics.dim_zone(location_id),
    provider_id             SMALLINT        NOT NULL REFERENCES analytics.dim_provider(provider_id),
    pickup_datetime         TIMESTAMP       NOT NULL,
    dropoff_datetime        TIMESTAMP       NOT NULL,
    request_datetime        TIMESTAMP,
    wait_time_minutes       NUMERIC(10,2),
    trip_miles              NUMERIC(10,3)   NOT NULL,
    trip_time_seconds       INTEGER         NOT NULL,
    trip_duration_minutes   NUMERIC(10,2)   NOT NULL,
    base_passenger_fare     NUMERIC(10,2)   NOT NULL,
    tolls                   NUMERIC(10,2)   NOT NULL DEFAULT 0,
    bcf                     NUMERIC(10,2)   NOT NULL DEFAULT 0,
    sales_tax               NUMERIC(10,2)   NOT NULL DEFAULT 0,
    congestion_surcharge    NUMERIC(10,2)   NOT NULL DEFAULT 0,
    airport_fee             NUMERIC(10,2)   NOT NULL DEFAULT 0,
    tips                    NUMERIC(10,2)   NOT NULL DEFAULT 0,
    driver_pay              NUMERIC(10,2)   NOT NULL,
    cbd_congestion_fee      NUMERIC(10,2)   NOT NULL DEFAULT 0,
    total_fare              NUMERIC(10,2)   NOT NULL,
    shared_request_flag     BOOLEAN,
    shared_match_flag       BOOLEAN,
    is_airport_trip         BOOLEAN         NOT NULL DEFAULT FALSE
);