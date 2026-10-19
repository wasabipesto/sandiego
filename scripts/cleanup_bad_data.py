"""
One-off cleanup of known-bad values written by the fetch app.

Steps (all run by default; name some to run only those):
    sleep_timezone  Fitbit sleep start/end times were stored as UTC but are local
                    wall time. Reinterpret them in $TZ. Only rows from the Fitbit
                    API (sleep_hours_asleep is set) are touched; older imports are
                    already correct. Refuses to run if the rows look already fixed.
    zero_steps      After the watch stopped reporting, Fitbit returned zero-filled
                    days, stored as steps_count_sum = 0. Set those to NULL in every
                    table, from the last bucket with heart-rate data onwards.
                    Refuses to run once later buckets have real steps (i.e. after the
                    Prometheus backfill), since zeros are then legitimate.
    zone_artifacts  The first daily bucket after an ingest gap got 0.0 hours for
                    both zones because no zone state was known. Set those to NULL.
    zone_stale      The person tracker stopped updating at a Home Assistant restart
                    on 2026-05-19, so later zone values are stale. Set them to NULL.

Dry run by default: prints what would change and rolls back. Pass --apply to commit.

Run this before scripts/backfill_prometheus.py. On the server:
    cd /opt/valinor && docker compose -f shalash-compose.yml run --rm --no-deps \\
        -v /opt/sandiego/scripts:/usr/src/scripts sandiego-fetch \\
        python scripts/cleanup_bad_data.py [--apply] [step ...]
"""

import argparse
import os
import sys

import psycopg2

STEPS = ["sleep_timezone", "zero_steps", "zone_artifacts", "zone_stale"]

# last_changed of the person tracker; it has not updated since this restart
ZONE_TRACKER_FROZEN_AT = "2026-05-19T22:39:52+00:00"


def get_database_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOSTNAME"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        database=os.environ.get("POSTGRES_DB", "sandiego"),
        user=os.environ.get("POSTGRES_USERNAME", "sandiego"),
        password=os.environ.get("POSTGRES_PASSWORD"),
    )


def sleep_timezone(cursor, tz):
    cursor.execute(
        """
        SELECT count(*),
            count(*) FILTER (
                WHERE extract(hour FROM sleep_time_start AT TIME ZONE %s) BETWEEN 15 AND 20
            )
        FROM daily_summary
        WHERE sleep_hours_asleep IS NOT NULL AND sleep_time_start IS NOT NULL
        """,
        (tz,),
    )
    total, evening = cursor.fetchone()
    print(f"  {total} Fitbit API nights, {evening} start between 15:00 and 20:59 local")
    if total == 0 or evening / total < 0.8:
        print("  Skipping: these do not look shifted (already fixed?)")
        return
    cursor.execute(
        """
        UPDATE daily_summary SET
            sleep_time_start = (sleep_time_start AT TIME ZONE 'UTC') AT TIME ZONE %(tz)s,
            sleep_time_end = (sleep_time_end AT TIME ZONE 'UTC') AT TIME ZONE %(tz)s
        WHERE sleep_hours_asleep IS NOT NULL
            AND (sleep_time_start IS NOT NULL OR sleep_time_end IS NOT NULL)
        """,
        {"tz": tz},
    )
    print(f"  Shifted {cursor.rowcount} rows")


def zero_steps(cursor):
    for table in ["daily_summary", "intraday_15m", "intraday_1m"]:
        cursor.execute(
            f"SELECT max(end_time) FROM {table} WHERE heart_rate_mean IS NOT NULL"
        )
        (cutoff,) = cursor.fetchone()
        if cutoff is None:
            print(f"  {table}: no heart-rate data, skipping")
            continue
        cursor.execute(
            f"""
            SELECT count(*) FROM {table}
            WHERE start_time >= %s + interval '1 day' AND steps_count_sum > 0
            """,
            (cutoff,),
        )
        (later_steps,) = cursor.fetchone()
        if later_steps:
            print(
                f"  {table}: {later_steps} buckets after {cutoff} already have steps,"
                " skipping (backfill already run?)"
            )
            continue
        cursor.execute(
            f"""
            UPDATE {table} SET steps_count_sum = NULL
            WHERE start_time >= %s AND steps_count_sum = 0
            """,
            (cutoff,),
        )
        print(f"  {table}: cleared {cursor.rowcount} zero-step buckets from {cutoff}")


def zone_artifacts(cursor):
    cursor.execute(
        """
        WITH zoned AS (
            SELECT id, start_time, zone_hours_home, zone_hours_work,
                lag(start_time) OVER w AS prev_start,
                lead(zone_hours_home) OVER w AS next_home
            FROM daily_summary
            WHERE zone_hours_home IS NOT NULL
            WINDOW w AS (ORDER BY start_time)
        )
        UPDATE daily_summary d SET zone_hours_home = NULL, zone_hours_work = NULL
        FROM zoned z
        WHERE d.id = z.id
            AND z.zone_hours_home = 0 AND z.zone_hours_work = 0
            AND z.start_time - z.prev_start > interval '1 day'
            AND z.next_home = 24
        RETURNING d.start_time
        """
    )
    rows = [row[0].date().isoformat() for row in cursor.fetchall()]
    print(f"  Cleared {len(rows)} days: {', '.join(rows)}")


def zone_stale(cursor):
    cursor.execute(
        """
        UPDATE daily_summary SET zone_hours_home = NULL, zone_hours_work = NULL
        WHERE end_time > %s
            AND (zone_hours_home IS NOT NULL OR zone_hours_work IS NOT NULL)
        """,
        (ZONE_TRACKER_FROZEN_AT,),
    )
    print(f"  daily_summary: cleared {cursor.rowcount} rows")
    cursor.execute(
        """
        UPDATE intraday_15m SET zone_select = NULL
        WHERE end_time > %s AND zone_select IS NOT NULL
        """,
        (ZONE_TRACKER_FROZEN_AT,),
    )
    print(f"  intraday_15m: cleared {cursor.rowcount} rows")


def main():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("steps", nargs="*", help="steps to run (default: all)")
    parser.add_argument("--apply", action="store_true", help="commit the changes")
    args = parser.parse_args()
    for step in args.steps:
        if step not in STEPS:
            parser.error(f"unknown step {step!r}, choose from {', '.join(STEPS)}")
    steps = args.steps or STEPS

    tz = os.environ.get("TZ")
    if "sleep_timezone" in steps and not tz:
        sys.exit("TZ must be set to the timezone the Fitbit account used")

    conn = get_database_connection()
    cursor = conn.cursor()
    for step in steps:
        print(step)
        if step == "sleep_timezone":
            sleep_timezone(cursor, tz)
        elif step == "zero_steps":
            zero_steps(cursor)
        elif step == "zone_artifacts":
            zone_artifacts(cursor)
        elif step == "zone_stale":
            zone_stale(cursor)

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
