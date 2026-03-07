-- ============================================================
-- Six BI views answering all required business questions
-- ============================================================

-- ============================================================
-- Q1 — Daily Demand Trend
-- Trips per calendar day + top 5 busiest days
-- ============================================================
CREATE OR REPLACE VIEW analytics.v_daily_demand AS
SELECT
    d.full_date,
    d.day_name,
    d.is_weekend,
    COUNT(*)                        AS total_trips,
    RANK() OVER (
        ORDER BY COUNT(*) DESC
    )                               AS demand_rank
FROM analytics.fact_trip f
JOIN analytics.dim_date d ON f.date_id = d.date_id
GROUP BY d.full_date, d.day_name, d.is_weekend
ORDER BY d.full_date;

-- Top 5 busiest days
CREATE OR REPLACE VIEW analytics.v_top5_busiest_days AS
SELECT
    d.full_date,
    d.day_name,
    d.is_weekend,
    COUNT(*)                        AS total_trips
FROM analytics.fact_trip f
JOIN analytics.dim_date d ON f.date_id = d.date_id
GROUP BY d.full_date, d.day_name, d.is_weekend
ORDER BY total_trips DESC
LIMIT 5;

-- ============================================================
-- Q2 — Peak Hours
-- Trips per hour of day — weekday vs weekend split
-- ============================================================
CREATE OR REPLACE VIEW analytics.v_peak_hours AS
SELECT
    t.hour,
    t.time_label,
    t.period_of_day,
    t.is_peak_hour,
    COUNT(*) FILTER (WHERE d.is_weekend = FALSE)    AS weekday_trips,
    COUNT(*) FILTER (WHERE d.is_weekend = TRUE)     AS weekend_trips,
    COUNT(*)                                        AS total_trips
FROM analytics.fact_trip f
JOIN analytics.dim_time t ON f.time_id = t.time_id
JOIN analytics.dim_date d ON f.date_id = d.date_id
GROUP BY t.hour, t.time_label, t.period_of_day, t.is_peak_hour
ORDER BY t.hour;

-- ============================================================
-- Q3 — Service Responsiveness
-- Avg wait time (minutes) by hour of day
-- ============================================================
CREATE OR REPLACE VIEW analytics.v_service_responsiveness AS
SELECT
    t.hour,
    t.time_label,
    t.period_of_day,
    t.is_peak_hour,
    COUNT(*)                                        AS total_trips,
    ROUND(AVG(f.wait_time_minutes), 2)              AS avg_wait_minutes,
    ROUND(MIN(f.wait_time_minutes), 2)              AS min_wait_minutes,
    ROUND(MAX(f.wait_time_minutes), 2)              AS max_wait_minutes,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY f.wait_time_minutes
    )::NUMERIC, 2)                                  AS median_wait_minutes
FROM analytics.fact_trip f
JOIN analytics.dim_time t ON f.time_id = t.time_id
WHERE f.wait_time_minutes IS NOT NULL
  AND f.wait_time_minutes >= 0
GROUP BY t.hour, t.time_label, t.period_of_day, t.is_peak_hour
ORDER BY t.hour;

-- ============================================================
-- Q4 — Trip Efficiency
-- Avg speed proxy per hour + flag unrealistic values
-- ============================================================
CREATE OR REPLACE VIEW analytics.v_trip_efficiency AS
SELECT
    t.hour,
    t.time_label,
    t.period_of_day,
    t.is_peak_hour,
    COUNT(*)                                        AS total_trips,
    ROUND(AVG(
        CASE
            WHEN f.trip_time_seconds > 0
            THEN (f.trip_miles / f.trip_time_seconds) * 3600
            ELSE NULL
        END
    ), 2)                                           AS avg_speed_mph,
    COUNT(*) FILTER (
        WHERE (f.trip_miles / NULLIF(f.trip_time_seconds, 0)) * 3600 > 100
    )                                               AS unrealistic_speed_count,
    ROUND(AVG(f.trip_miles), 3)                     AS avg_trip_miles,
    ROUND(AVG(f.trip_duration_minutes), 2)          AS avg_duration_minutes
FROM analytics.fact_trip f
JOIN analytics.dim_time t ON f.time_id = t.time_id
WHERE f.trip_time_seconds > 0
  AND f.trip_miles > 0
GROUP BY t.hour, t.time_label, t.period_of_day, t.is_peak_hour
ORDER BY t.hour;

-- ============================================================
-- Q5 — Revenue Components
-- Total fare and tips by day + tips as % of fare
-- ============================================================
CREATE OR REPLACE VIEW analytics.v_revenue_components AS
SELECT
    d.full_date,
    d.day_name,
    d.is_weekend,
    COUNT(*)                                        AS total_trips,
    ROUND(SUM(f.base_passenger_fare), 2)            AS total_base_fare,
    ROUND(SUM(f.tips), 2)                           AS total_tips,
    ROUND(SUM(f.total_fare), 2)                     AS total_revenue,
    ROUND(SUM(f.driver_pay), 2)                     AS total_driver_pay,
    ROUND(SUM(f.tolls), 2)                          AS total_tolls,
    ROUND(SUM(f.congestion_surcharge), 2)           AS total_congestion,
    ROUND(SUM(f.cbd_congestion_fee), 2)             AS total_cbd_fee,
    ROUND(
        SUM(f.tips) / NULLIF(SUM(f.base_passenger_fare), 0) * 100
    , 2)                                            AS tips_pct_of_fare
FROM analytics.fact_trip f
JOIN analytics.dim_date d ON f.date_id = d.date_id
GROUP BY d.full_date, d.day_name, d.is_weekend
ORDER BY d.full_date;

-- ============================================================
-- Q6 — Location Hot-Spots
-- Top 10 pickup and dropoff zones by trip count
-- ============================================================
CREATE OR REPLACE VIEW analytics.v_top_pickup_zones AS
SELECT
    pu.location_id,
    pu.zone_name                                    AS pickup_zone,
    pu.borough                                      AS pickup_borough,
    COUNT(*)                                        AS total_trips,
    ROUND(AVG(f.trip_miles), 2)                     AS avg_trip_miles,
    ROUND(AVG(f.base_passenger_fare), 2)            AS avg_fare,
    RANK() OVER (ORDER BY COUNT(*) DESC)            AS pickup_rank
FROM analytics.fact_trip f
JOIN analytics.dim_zone pu ON f.pu_location_id = pu.location_id
GROUP BY pu.location_id, pu.zone_name, pu.borough
ORDER BY total_trips DESC
LIMIT 10;

CREATE OR REPLACE VIEW analytics.v_top_dropoff_zones AS
SELECT
    do_z.location_id,
    do_z.zone_name                                  AS dropoff_zone,
    do_z.borough                                    AS dropoff_borough,
    COUNT(*)                                        AS total_trips,
    ROUND(AVG(f.trip_miles), 2)                     AS avg_trip_miles,
    ROUND(AVG(f.base_passenger_fare), 2)            AS avg_fare,
    RANK() OVER (ORDER BY COUNT(*) DESC)            AS dropoff_rank
FROM analytics.fact_trip f
JOIN analytics.dim_zone do_z ON f.do_location_id = do_z.location_id
GROUP BY do_z.location_id, do_z.zone_name, do_z.borough
ORDER BY total_trips DESC
LIMIT 10;

-- Combined pickup + dropoff hot-spots
CREATE OR REPLACE VIEW analytics.v_location_hotspots AS
SELECT
    pu.zone_name                                    AS pickup_zone,
    pu.borough                                      AS pickup_borough,
    do_z.zone_name                                  AS dropoff_zone,
    do_z.borough                                    AS dropoff_borough,
    COUNT(*)                                        AS trip_count,
    ROUND(AVG(f.trip_miles), 2)                     AS avg_miles
FROM analytics.fact_trip f
JOIN analytics.dim_zone pu  ON f.pu_location_id = pu.location_id
JOIN analytics.dim_zone do_z ON f.do_location_id = do_z.location_id
GROUP BY pu.zone_name, pu.borough, do_z.zone_name, do_z.borough
ORDER BY trip_count DESC
LIMIT 10;