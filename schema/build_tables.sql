DROP TABLE IF EXISTS daily_summary;
CREATE TABLE daily_summary (
    id SERIAL PRIMARY KEY,
    start_time TIMESTAMPTZ UNIQUE,
    end_time TIMESTAMPTZ UNIQUE,
    sleep_hours_inbed DECIMAL,
    sleep_hours_asleep DECIMAL,
    sleep_hours_deep DECIMAL,
    sleep_hours_light DECIMAL,
    sleep_hours_rem DECIMAL,
    sleep_hours_wake DECIMAL,
    sleep_time_start TIMESTAMPTZ,
    sleep_time_end TIMESTAMPTZ,
    steps_count_sum INTEGER,
    heart_rate_mean DECIMAL,
    heart_rate_pct_10 DECIMAL,
    heart_rate_pct_20 DECIMAL,
    heart_rate_pct_50 DECIMAL,
    heart_rate_pct_80 DECIMAL,
    heart_rate_pct_90 DECIMAL,
    heart_rate_rmssd DECIMAL,
    zone_hours_home DECIMAL,
    zone_hours_work DECIMAL,
    activity_hours_still DECIMAL,
    activity_hours_walking DECIMAL,
    activity_hours_running DECIMAL,
    activity_hours_vehicle DECIMAL,
    device_hours_phone DECIMAL,
    device_hours_desktop DECIMAL,
    device_hours_tv DECIMAL
);
--
DROP TABLE IF EXISTS intraday_15m;
CREATE TABLE intraday_15m (
    id SERIAL PRIMARY KEY,
    start_time TIMESTAMPTZ UNIQUE,
    end_time TIMESTAMPTZ UNIQUE,
    steps_count_sum INTEGER,
    heart_rate_mean DECIMAL,
    zone_select VARCHAR,
    activity_select VARCHAR,
    device_hours_phone DECIMAL,
    device_hours_desktop DECIMAL,
    device_hours_tv DECIMAL
);
--
DROP TABLE IF EXISTS intraday_1m;
CREATE TABLE intraday_1m (
    id SERIAL PRIMARY KEY,
    start_time TIMESTAMPTZ UNIQUE,
    end_time TIMESTAMPTZ UNIQUE,
    steps_count_sum INTEGER,
    heart_rate_mean DECIMAL
);