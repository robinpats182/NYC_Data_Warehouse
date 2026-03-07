-- Builds analytics star schema from modeled layer
-- modeled.fhv_trip_clean → analytics fact + dimensions

-- STEP 1 — dim_date
TRUNCATE TABLE analytics.dim_date RESTART IDENTITY CASCADE;

INSERT INTO analytics.dim_date (
    date_id,
    full_date,
    year,
    month,
    month_name,
    day,
    day_of_week,
    day_name,
    is_weekend,
    week_of_year
)
SELECT DISTINCT
    TO_CHAR(pickup_datetime::DATE, 'YYYYMMDD')::INTEGER     AS date_id,
    pickup_datetime::DATE                                   AS full_date,
    EXTRACT(YEAR    FROM pickup_datetime)::SMALLINT         AS year,
    EXTRACT(MONTH   FROM pickup_datetime)::SMALLINT         AS month,
    TO_CHAR(pickup_datetime, 'Month')                       AS month_name,
    EXTRACT(DAY     FROM pickup_datetime)::SMALLINT         AS day,
    EXTRACT(DOW     FROM pickup_datetime)::SMALLINT         AS day_of_week,
    TO_CHAR(pickup_datetime, 'Day')                         AS day_name,
    EXTRACT(DOW     FROM pickup_datetime) IN (0, 6)         AS is_weekend,
    EXTRACT(WEEK    FROM pickup_datetime)::SMALLINT         AS week_of_year
FROM modeled.fhv_trip_clean
ORDER BY full_date;

-- STEP 2 — dim_time
TRUNCATE TABLE analytics.dim_time RESTART IDENTITY CASCADE;

INSERT INTO analytics.dim_time (
    time_id,
    hour,
    time_label,
    period_of_day,
    is_peak_hour
)
SELECT
    hour::SMALLINT                                          AS time_id,
    hour::SMALLINT                                          AS hour,
    LPAD(hour::TEXT, 2, '0') || ':00'                      AS time_label,
    CASE
        WHEN hour BETWEEN 5  AND 11 THEN 'Morning'
        WHEN hour BETWEEN 12 AND 16 THEN 'Afternoon'
        WHEN hour BETWEEN 17 AND 20 THEN 'Evening'
        ELSE 'Night'
    END                                                     AS period_of_day,
    CASE
        WHEN hour BETWEEN 7 AND 9   THEN TRUE
        WHEN hour BETWEEN 16 AND 19 THEN TRUE
        ELSE FALSE
    END                                                     AS is_peak_hour
FROM generate_series(0, 23) AS hour;

-- STEP 3 — dim_zone
TRUNCATE TABLE analytics.dim_zone RESTART IDENTITY CASCADE;

-- Load from taxi zone lookup CSV via Python (see src/load_zones.py)
-- This is a placeholder — run load_zones.py first before this step

-- STEP 4 — dim_provider
TRUNCATE TABLE analytics.dim_provider RESTART IDENTITY CASCADE;

INSERT INTO analytics.dim_provider (
    hvfhs_license_num,
    provider_name
)
SELECT DISTINCT
    hvfhs_license_num,
    CASE hvfhs_license_num
        WHEN 'HV0002' THEN 'Juno'
        WHEN 'HV0003' THEN 'Uber'
        WHEN 'HV0004' THEN 'Via'
        WHEN 'HV0005' THEN 'Lyft'
        ELSE 'Unknown'
    END AS provider_name
FROM modeled.fhv_trip_clean
ORDER BY hvfhs_license_num;

-- STEP 5 — fact_trip
TRUNCATE TABLE analytics.fact_trip RESTART IDENTITY CASCADE;

INSERT INTO analytics.fact_trip (
    trip_id,
    date_id,
    time_id,
    pu_location_id,
    do_location_id,
    provider_id,
    pickup_datetime,
    dropoff_datetime,
    request_datetime,
    wait_time_minutes,
    trip_miles,
    trip_time_seconds,
    trip_duration_minutes,
    base_passenger_fare,
    tolls,
    bcf,
    sales_tax,
    congestion_surcharge,
    airport_fee,
    tips,
    driver_pay,
    cbd_congestion_fee,
    total_fare,
    shared_request_flag,
    shared_match_flag,
    is_airport_trip
)
SELECT
    c.trip_id,
    TO_CHAR(c.pickup_datetime::DATE, 'YYYYMMDD')::INTEGER   AS date_id,
    EXTRACT(HOUR FROM c.pickup_datetime)::SMALLINT           AS time_id,
    c.pu_location_id,
    c.do_location_id,
    p.provider_id,
    c.pickup_datetime,
    c.dropoff_datetime,
    c.request_datetime,
    c.wait_time_minutes,
    c.trip_miles,
    c.trip_time_seconds,
    c.trip_duration_minutes,
    c.base_passenger_fare,
    c.tolls,
    c.bcf,
    c.sales_tax,
    c.congestion_surcharge,
    c.airport_fee,
    c.tips,
    c.driver_pay,
    c.cbd_congestion_fee,
    c.total_fare,
    c.shared_request_flag,
    c.shared_match_flag,
    -- Airport trip flag: JFK=132, LaGuardia=138, Newark=1
    (c.pu_location_id IN (1, 132, 138) OR 
     c.do_location_id IN (1, 132, 138))                     AS is_airport_trip
FROM modeled.fhv_trip_clean c
JOIN analytics.dim_provider p 
    ON c.hvfhs_license_num = p.hvfhs_license_num;