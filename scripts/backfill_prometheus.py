"""
Backfill daily_summary and intraday_15m from Home Assistant metrics in Prometheus.

Metrics (all run by default; name some to run only those):
    sleep            sleep_time_start, sleep_time_end, sleep_hours_inbed from phone
                     charging: the longest charging session that starts between
                     19:00 and 05:59 local and lasts at least 3 hours (bridging
                     unplugs under 10 minutes), assigned to the day it ends. Daily
                     only; rows with any sleep value are kept.
    steps            steps_count_sum from the phone's daily step counter. Each
                     increase is spread over the time since the previous one (at
                     most an hour, but at no more than 130 steps/minute). Daily and
                     15-minute buckets.
    climate_indoor   climate_indoor_* from the thermostat.
    climate_outdoor  climate_outdoor_* from the outdoor sensor.

Only NULL values are filled, so existing data is never overwritten; run
scripts/cleanup_bad_data.py first so zero-filled steps are NULL. Missing bucket
rows in the window are inserted. Buckets overlapping a gap in the Prometheus data
are left alone. Safe to re-run.

Dry run by default: prints what would change and rolls back. Pass --apply to commit.

On the server:
    cd /opt/valinor && docker compose -f shalash-compose.yml run --rm --no-deps \\
        -v /opt/sandiego/scripts:/usr/src/scripts sandiego-fetch \\
        python scripts/backfill_prometheus.py [--apply] [--start DATE] [metric ...]
"""

import argparse
import bisect
import os
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg2
import requests
import yaml
from psycopg2.extras import execute_values

METRICS = ["sleep", "steps", "climate_indoor", "climate_outdoor"]

BATTERY_LEVEL = 'hassio_sensor_battery_percent{entity="sensor.leandros_battery_level"}'
BATTERY_STATE_UPDATED = (
    'hassio_last_updated_time_seconds{entity="sensor.leandros_battery_state"}'
)
DAILY_STEPS = 'hassio_sensor_unit_steps{entity="sensor.leandros_daily_steps"}'
CLIMATE = {
    "climate_indoor": {
        "temperature": 'hassio_sensor_temperature_celsius{entity="sensor.nest_temperature"}',
        "humidity": 'hassio_sensor_humidity_percent{entity="sensor.nest_humidity"}',
    },
    "climate_outdoor": {
        "temperature": 'hassio_sensor_temperature_celsius{entity="sensor.keye_temperature"}',
        "humidity": 'hassio_sensor_humidity_percent{entity="sensor.keye_humidity"}',
    },
}

SCRAPE_INTERVAL = 30
# a longer stretch without samples counts as missing data
MAX_SAMPLE_GAP = timedelta(minutes=10)
# fraction of expected samples a bucket needs for climate aggregates
MIN_CLIMATE_COVERAGE = 0.75

SLEEP_START_HOURS = (19, 6)  # local, [19:00, 06:00)
SLEEP_MIN_DURATION = timedelta(hours=3)
# charging sessions separated by a shorter unplug are treated as one
SESSION_MERGE_GAP = timedelta(minutes=10)

# a step counter drop below this fraction that persists is a daily reset
STEP_RESET_FRACTION = 0.5
STEP_RESET_PERSIST = timedelta(minutes=30)
STEP_SPREAD_MAX = timedelta(hours=1)
# synced batches (e.g. a recorded walk) are spread at no more than this rate
STEP_MAX_CADENCE = 130  # steps per minute


def get_database_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOSTNAME"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        database=os.environ.get("POSTGRES_DB", "sandiego"),
        user=os.environ.get("POSTGRES_USERNAME", "sandiego"),
        password=os.environ.get("POSTGRES_PASSWORD"),
    )


def prometheus(path, params):
    url = os.environ.get("PROMETHEUS_URL", "http://prometheus:9090")
    response = requests.get(url + "/api/v1/" + path, params=params, timeout=300)
    response.raise_for_status()
    return response.json()["data"]["result"]


def get_samples(selector, start, end):
    """Raw samples as a sorted list of (datetime, float)."""
    samples = {}
    chunk_end = end
    while chunk_end > start:
        chunk = min(timedelta(days=7), chunk_end - start)
        result = prometheus(
            "query",
            {
                "query": f"{selector}[{int(chunk.total_seconds())}s]",
                "time": chunk_end.timestamp(),
            },
        )
        for series in result:
            for t, v in series["values"]:
                samples[float(t)] = float(v)
        chunk_end -= chunk
    return [
        (datetime.fromtimestamp(t, timezone.utc), samples[t]) for t in sorted(samples)
    ]


def get_sample_gaps(samples):
    gaps = [
        (a[0], b[0]) for a, b in zip(samples, samples[1:]) if b[0] - a[0] > MAX_SAMPLE_GAP
    ]
    return gaps


def overlaps_gap(start, end, gaps, data_start, data_end):
    if start < data_start or end > data_end:
        return True
    return any(gap_start < end and gap_end > start for gap_start, gap_end in gaps)


def get_buckets(start, end, table_config):
    """Bucket start times in [start, end) on the grid configured for the table."""
    width = timedelta(minutes=table_config["duration_minutes"])
    offset = timedelta(minutes=table_config["align_offset_minutes"])
    bucket = start.replace(hour=0, minute=0, second=0, microsecond=0) + offset
    while bucket < start:
        bucket += width
    buckets = []
    while bucket + width <= end:
        buckets.append(bucket)
        bucket += width
    return buckets, width


def insert_buckets(cursor, table, buckets, width):
    execute_values(
        cursor,
        f"INSERT INTO {table} (start_time, end_time) VALUES %s ON CONFLICT DO NOTHING",
        [(b, b + width) for b in buckets],
        page_size=max(len(buckets), 1),  # one statement, so rowcount covers all rows
    )
    print(f"  {table}: inserted {cursor.rowcount} missing buckets")


def fill_nulls(cursor, table, column, cast, values):
    """Set column for buckets keyed by start time, only where it is NULL."""
    if not values:
        print(f"  {table}.{column}: no values")
        return
    execute_values(
        cursor,
        f"""
        UPDATE {table} t SET {column} = v.value
        FROM (VALUES %s) AS v(start_time, value)
        WHERE t.start_time = v.start_time AND t.{column} IS NULL
        """,
        list(values.items()),
        template=f"(%s::timestamptz, %s::{cast})",
        page_size=len(values),  # one statement, so rowcount covers all rows
    )
    print(f"  {table}.{column}: filled {cursor.rowcount} of {len(values)} computed")


def get_charging_sessions(level, state_updated):
    """(start, end) of periods the phone was plugged in."""
    level_times = [t for t, _ in level]

    def level_at(t):
        return level[max(bisect.bisect_right(level_times, t) - 1, 0)][1]

    # battery state has no numeric value in Prometheus, only its last-updated time;
    # between two transitions the phone was plugged in if the level went up, or
    # stayed at the top
    transitions = sorted(
        {
            datetime.fromtimestamp(v, timezone.utc)
            for _, v in state_updated
            if level_times[0] <= datetime.fromtimestamp(v, timezone.utc) <= level_times[-1]
        }
    )
    sessions = []
    for a, b in zip(transitions, transitions[1:]):
        level_start = level_at(a + timedelta(seconds=90))
        level_end = level_at(b - timedelta(seconds=30))
        i, j = bisect.bisect_left(level_times, a), bisect.bisect_right(level_times, b)
        level_max = max([v for _, v in level[i:j]] or [level_start])
        plugged = level_end > level_start or (level_max >= 99 and level_end >= level_start)
        if not plugged:
            continue
        if sessions and a - sessions[-1][1] < SESSION_MERGE_GAP:
            sessions[-1][1] = b
        else:
            sessions.append([a, b])
    return [tuple(s) for s in sessions]


def compute_sleep(start, end, tz):
    level = get_samples(BATTERY_LEVEL, start, end)
    state_updated = get_samples(BATTERY_STATE_UPDATED, start, end)
    if not level or not state_updated:
        return {}
    gaps = get_sample_gaps(level)
    nights = {}
    for a, b in get_charging_sessions(level, state_updated):
        start_hour = a.astimezone(tz).hour
        if not (start_hour >= SLEEP_START_HOURS[0] or start_hour < SLEEP_START_HOURS[1]):
            continue
        if b - a < SLEEP_MIN_DURATION:
            continue
        margin = timedelta(hours=1)
        if overlaps_gap(a - margin, b + margin, gaps, level[0][0], level[-1][0]):
            continue
        wake_date = b.astimezone(tz).date()
        if wake_date not in nights or b - a > nights[wake_date][1] - nights[wake_date][0]:
            nights[wake_date] = (a, b)
    return nights


def get_step_increments(samples):
    """(time, steps) increments from a counter that resets daily and glitches low."""
    increments = []
    running_max = None
    times = [t for t, _ in samples]
    for i, (t, value) in enumerate(samples):
        if running_max is None:
            running_max = value  # baseline, steps before the data starts are unknown
            continue
        if value < running_max * STEP_RESET_FRACTION:
            j = bisect.bisect_right(times, t + STEP_RESET_PERSIST)
            if all(v < running_max * STEP_RESET_FRACTION for _, v in samples[i:j]):
                # counter reset: everything up to value is new
                increments.append((t, value))
                running_max = value
            continue
        if value > running_max:
            increments.append((t, value - running_max))
            running_max = value
    return increments


def compute_steps(start, end, bucket_starts, width):
    samples = get_samples(DAILY_STEPS, start - STEP_SPREAD_MAX, end + STEP_SPREAD_MAX)
    if not samples:
        return {}
    gaps = get_sample_gaps(samples)
    totals = {b: 0.0 for b in bucket_starts}
    previous = None
    for t, steps in get_step_increments(samples):
        spread = min(t - previous, STEP_SPREAD_MAX) if previous else STEP_SPREAD_MAX
        spread = max(spread, timedelta(minutes=steps / STEP_MAX_CADENCE))
        spread_start = t - spread
        previous = t
        duration = spread.total_seconds()
        if duration <= 0:
            continue
        rate = steps / duration
        # distribute over the buckets the spread interval touches
        first = bisect.bisect_right(bucket_starts, spread_start) - 1
        for b in bucket_starts[max(first, 0):]:
            if b >= t:
                break
            overlap = (min(b + width, t) - max(b, spread_start)).total_seconds()
            if overlap > 0:
                totals[b] += rate * overlap
    return {
        b: round(total)
        for b, total in totals.items()
        if not overlaps_gap(b, b + width, gaps, samples[0][0], samples[-1][0])
    }


def compute_climate(selector, kind, bucket_starts, width):
    """{aggregate: {bucket start: value}} for mean, maximum, minimum."""
    if not bucket_starts:
        return {}
    window = f"{int(width.total_seconds())}s"
    convert = (lambda c: c * 9 / 5 + 32) if kind == "temperature" else (lambda x: x)
    queries = {
        "count": f"count_over_time({selector}[{window}])",
        "mean": f"avg_over_time({selector}[{window}])",
        "maximum": f"max_over_time({selector}[{window}])",
        "minimum": f"min_over_time({selector}[{window}])",
    }
    results = {name: {} for name in queries}
    # evaluate at bucket ends, in chunks under the API's point limit
    chunk = 10000
    for i in range(0, len(bucket_starts), chunk):
        chunk_starts = bucket_starts[i:i + chunk]
        for name, query in queries.items():
            for series in prometheus(
                "query_range",
                {
                    "query": query,
                    "start": (chunk_starts[0] + width).timestamp(),
                    "end": (chunk_starts[-1] + width).timestamp(),
                    "step": window,
                },
            ):
                for t, v in series["values"]:
                    bucket = datetime.fromtimestamp(float(t), timezone.utc) - width
                    results[name][bucket] = float(v)
    expected = width.total_seconds() / SCRAPE_INTERVAL
    covered = {b for b, n in results["count"].items() if n >= MIN_CLIMATE_COVERAGE * expected}
    return {
        name: {
            b: round(convert(v), 4 if name == "mean" else 2)
            for b, v in results[name].items()
            if b in covered
        }
        for name in ["mean", "maximum", "minimum"]
    }


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("metrics", nargs="*", help="metrics to run (default: all)")
    parser.add_argument(
        "--start",
        type=datetime.fromisoformat,
        help="local date/time to start from (default: one year ago)",
    )
    parser.add_argument(
        "--end", type=datetime.fromisoformat, help="local date/time to end at (default: now)"
    )
    parser.add_argument("--apply", action="store_true", help="commit the changes")
    args = parser.parse_args()
    for metric in args.metrics:
        if metric not in METRICS:
            parser.error(f"unknown metric {metric!r}, choose from {', '.join(METRICS)}")
    metrics = args.metrics or METRICS

    if not os.environ.get("TZ"):
        sys.exit("TZ must be set to the local timezone")
    tz = ZoneInfo(os.environ["TZ"])
    now = datetime.now(timezone.utc)
    end = args.end.replace(tzinfo=tz) if args.end else now
    start = args.start.replace(tzinfo=tz) if args.start else end - timedelta(days=365)
    end, start = min(end, now).astimezone(timezone.utc), start.astimezone(timezone.utc)
    print(f"Backfilling {start.isoformat()} to {end.isoformat()}")

    with open("configuration.yml", "r") as file:
        table_config = yaml.safe_load(file)["tables"]
    daily, daily_width = get_buckets(start, end, table_config["daily_summary"])
    intraday, intraday_width = get_buckets(start, end, table_config["intraday_15m"])

    conn = get_database_connection()
    cursor = conn.cursor()
    insert_buckets(cursor, "daily_summary", daily, daily_width)
    insert_buckets(cursor, "intraday_15m", intraday, intraday_width)

    for metric in metrics:
        print(metric)
        if metric == "sleep":
            nights = compute_sleep(start, end, tz)
            # only rows with no sleep data at all, so one night never mixes sources
            cursor.execute(
                """
                SELECT start_time FROM daily_summary
                WHERE start_time >= %s AND start_time < %s
                    AND sleep_time_start IS NULL AND sleep_time_end IS NULL
                    AND sleep_hours_inbed IS NULL
                """,
                (start, end),
            )
            empty = {row[0].astimezone(tz).date(): row[0] for row in cursor.fetchall()}
            matched = {empty[d]: s for d, s in nights.items() if d in empty}
            print(f"  {len(nights)} nights found, {len(matched)} for days with no sleep data")
            fill_nulls(cursor, "daily_summary", "sleep_time_start", "timestamptz",
                       {b: s[0] for b, s in matched.items()})
            fill_nulls(cursor, "daily_summary", "sleep_time_end", "timestamptz",
                       {b: s[1] for b, s in matched.items()})
            fill_nulls(cursor, "daily_summary", "sleep_hours_inbed", "numeric",
                       {b: (s[1] - s[0]).total_seconds() / 3600 for b, s in matched.items()})
        elif metric == "steps":
            for table, buckets, width in [
                ("daily_summary", daily, daily_width),
                ("intraday_15m", intraday, intraday_width),
            ]:
                fill_nulls(cursor, table, "steps_count_sum", "integer",
                           compute_steps(start, end, buckets, width))
        elif metric in CLIMATE:
            for kind, selector in CLIMATE[metric].items():
                aggregates = compute_climate(selector, kind, daily, daily_width)
                for name, values in aggregates.items():
                    suffix = {"mean": "mean", "maximum": "high", "minimum": "low"}[name]
                    fill_nulls(cursor, "daily_summary", f"{metric}_{kind}_{suffix}",
                               "numeric", values)
                aggregates = compute_climate(selector, kind, intraday, intraday_width)
                fill_nulls(cursor, "intraday_15m", f"{metric}_{kind}_mean", "numeric",
                           aggregates.get("mean", {}))

    if args.apply:
        conn.commit()
        print("Committed.")
    else:
        conn.rollback()
        print("Dry run, rolled back. Pass --apply to commit.")
    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
