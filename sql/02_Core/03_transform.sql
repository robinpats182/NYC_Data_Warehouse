-- Transforms staging data into modeled layer
-- staging.fhv_trip_raw → modeled.fhv_trip_clean

-- Drop and recreate for re-runability
DROP TABLE IF EXISTS modeled.fhv_trip_clean;

CREATE TABLE modeled.fhv_trip_clean AS
SELECT
    -- Provider info
    hvfhs_license_num,
    dispatching_base_num,
    originating_base_num,

    -- Timestamps
    request_datetime,
    on_scene_datetime,
    pickup_datetime,
    dropoff_datetime,

    -- Locations
    PULocationID::SMALLINT                                      AS pu_location_id,
    DOLocationID::SMALLINT                                      AS do_location_id,

    -- Trip metrics
    ROUND(trip_miles::NUMERIC, 3)                               AS trip_miles,
    trip_time::INTEGER                                          AS trip_time_seconds,
    ROUND(trip_time::NUMERIC / 60, 2)                          AS trip_duration_minutes,

    -- Wait time
    CASE
        WHEN request_datetime IS NOT NULL AND pickup_datetime IS NOT NULL
        THEN ROUND(EXTRACT(EPOCH FROM (pickup_datetime - request_datetime))::NUMERIC / 60, 2)
        ELSE NULL
    END                                                         AS wait_time_minutes,

    -- Fare components
    ROUND(base_passenger_fare::NUMERIC, 2)                     AS base_passenger_fare,
    ROUND(COALESCE(tolls, 0)::NUMERIC, 2)                      AS tolls,
    ROUND(COALESCE(bcf, 0)::NUMERIC, 2)                        AS bcf,
    ROUND(COALESCE(sales_tax, 0)::NUMERIC, 2)                  AS sales_tax,
    ROUND(COALESCE(congestion_surcharge, 0)::NUMERIC, 2)       AS congestion_surcharge,
    ROUND(COALESCE(airport_fee, 0)::NUMERIC, 2)                AS airport_fee,
    ROUND(COALESCE(tips, 0)::NUMERIC, 2)                       AS tips,
    ROUND(driver_pay::NUMERIC, 2)                              AS driver_pay,
    ROUND(COALESCE(cbd_congestion_fee, 0)::NUMERIC, 2)         AS cbd_congestion_fee,

    -- Total fare
    ROUND((
        COALESCE(base_passenger_fare, 0) +
        COALESCE(tolls, 0) +
        COALESCE(bcf, 0) +
        COALESCE(sales_tax, 0) +
        COALESCE(congestion_surcharge, 0) +
        COALESCE(airport_fee, 0) +
        COALESCE(tips, 0) +
        COALESCE(cbd_congestion_fee, 0)
    )::NUMERIC, 2)                                             AS total_fare,

    -- Convert flags Y/N to boolean
    (shared_request_flag = 'Y')                                AS shared_request_flag,
    (shared_match_flag   = 'Y')                                AS shared_match_flag,
    (access_a_ride_flag  = 'Y')                                AS access_a_ride_flag,
    (wav_request_flag    = 'Y')                                AS wav_request_flag,
    (wav_match_flag      = 'Y')                                AS wav_match_flag

FROM staging.fhv_trip_raw

WHERE
    pickup_datetime         IS NOT NULL
    AND dropoff_datetime    IS NOT NULL
    AND PULocationID        IS NOT NULL
    AND DOLocationID        IS NOT NULL
    AND base_passenger_fare IS NOT NULL
    AND driver_pay          IS NOT NULL
    AND trip_miles          >= 0
    AND trip_time           >= 0
    AND dropoff_datetime    >= pickup_datetime
    AND (
        request_datetime IS NULL
        OR pickup_datetime - request_datetime <= INTERVAL '2 hours'
    );

-- Add primary key after table creation (faster than during insert)
ALTER TABLE modeled.fhv_trip_clean ADD COLUMN trip_id BIGSERIAL PRIMARY KEY;

-- Add indexes for join performance in next steps
CREATE INDEX idx_clean_pickup_dt    ON modeled.fhv_trip_clean (pickup_datetime);
CREATE INDEX idx_clean_pu_location  ON modeled.fhv_trip_clean (pu_location_id);
CREATE INDEX idx_clean_do_location  ON modeled.fhv_trip_clean (do_location_id);
CREATE INDEX idx_clean_license      ON modeled.fhv_trip_clean (hvfhs_license_num);