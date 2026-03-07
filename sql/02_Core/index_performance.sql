-- Performance analysis — EXPLAIN plans + indexes
-- Run each section separately in pgAdmin

-- ============================================================
-- STEP 1 — BEFORE INDEXES
-- Run these EXPLAIN queries and save the output
-- ============================================================

-- Q1 — Daily Demand (heavy aggregation on pickup_datetime)
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    d.full_date,
    COUNT(*) AS total_trips
FROM analytics.fact_trip f
JOIN analytics.dim_date d ON f.date_id = d.date_id
GROUP BY d.full_date
ORDER BY d.full_date;

-- Q6 — Location Hot-Spots (heavy join on pu_location_id)
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    pu.zone_name,
    pu.borough,
    COUNT(*) AS trip_count
FROM analytics.fact_trip f
JOIN analytics.dim_zone pu ON f.pu_location_id = pu.location_id
GROUP BY pu.zone_name, pu.borough
ORDER BY trip_count DESC
LIMIT 10;

-- ============================================================
-- STEP 2 — ADD INDEXES
-- ============================================================

-- Index on date_id for daily aggregations (Q1, Q2, Q5)
CREATE INDEX IF NOT EXISTS idx_fact_date_id
    ON analytics.fact_trip (date_id);

-- Index on pu_location_id for zone hot-spots (Q6)
CREATE INDEX IF NOT EXISTS idx_fact_pu_location
    ON analytics.fact_trip (pu_location_id);

-- Index on do_location_id for zone hot-spots (Q6)
CREATE INDEX IF NOT EXISTS idx_fact_do_location
    ON analytics.fact_trip (do_location_id);

-- Index on time_id for peak hour queries (Q2, Q3, Q4)
CREATE INDEX IF NOT EXISTS idx_fact_time_id
    ON analytics.fact_trip (time_id);

-- Index on pickup_datetime for range queries
CREATE INDEX IF NOT EXISTS idx_fact_pickup_dt
    ON analytics.fact_trip (pickup_datetime);

-- ============================================================
-- STEP 3 — AFTER INDEXES
-- Run the same EXPLAIN queries again and compare
-- ============================================================

-- Q1 — Daily Demand after indexing
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    d.full_date,
    COUNT(*) AS total_trips
FROM analytics.fact_trip f
JOIN analytics.dim_date d ON f.date_id = d.date_id
GROUP BY d.full_date
ORDER BY d.full_date;

-- Q6 — Location Hot-Spots after indexing
EXPLAIN (ANALYZE, BUFFERS)
SELECT
    pu.zone_name,
    pu.borough,
    COUNT(*) AS trip_count
FROM analytics.fact_trip f
JOIN analytics.dim_zone pu ON f.pu_location_id = pu.location_id
GROUP BY pu.zone_name, pu.borough
ORDER BY trip_count DESC
LIMIT 10;