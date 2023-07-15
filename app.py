import json
import os
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import requests
from psycopg2.extensions import AsIs


def get_data_hass(start_time, sensors):
    sensors_string = ",".join([s for s in sensors])
    response = requests.get(
        os.environ.get("HASS_URL") + "/api/history/period/" + start_time.isoformat(),
        headers={
            "authorization": "Bearer " + os.environ.get("HASS_API_KEY"),
            "content-type": "application/json",
        },
        params={
            "filter_entity_id": sensors_string,
            "end_time": datetime.now(timezone.utc).isoformat(),
            "no_attributes": True,
        },
    )
    return response.json()


def format_hass_timestamps(response):
    data_return = []
    for list_item in response:
        converted_list = []
        for item in list_item:
            converted_item = {}
            for key, value in item.items():
                if key in ["last_changed", "last_updated"]:
                    converted_item[key] = datetime.fromisoformat(value)
                else:
                    converted_item[key] = value
            converted_list.append(converted_item)
        data_return.append(converted_list)
    return data_return


def get_data_fitbit(url):
    # load secrets file
    with open("./secrets/fitbit.json", "r") as file:
        fitbit_secrets = json.load(file)

    # pull data from api
    response = requests.get(
        url,
        headers={"authorization": "Bearer " + fitbit_secrets["access_token"]},
        params={"timezone": "UTC"},
    )

    if response.status_code == 429:
        raise Exception("Fitbit API rate-limited.", response.headers)

    if response.status_code == 401:
        # request new key
        response = requests.post(
            "https://api.fitbit.com/oauth2/token",
            headers={
                "authorization": "Basic " + fitbit_secrets["basic_token"],
                "content-type": "application/x-www-form-urlencoded",
            },
            data={
                "grant_type": "refresh_token",
                "client_id": fitbit_secrets["client_id"],
                "refresh_token": fitbit_secrets["refresh_token"],
            },
        ).json()

        # update secrets file
        fitbit_secrets.update(
            {
                "access_token": response["access_token"],
                "refresh_token": response["refresh_token"],
            }
        )
        with open("./secrets/fitbit.json", "w") as file:
            json.dump(fitbit_secrets, file)

        # try again
        response = requests.get(
            url,
            headers={"authorization": "Bearer " + fitbit_secrets["access_token"]},
            params={"timezone": "UTC"},
        )
    return response.json()


def get_last_timestamp(conn, table):
    cursor = conn.cursor()
    cursor.execute(f"SELECT timestamp FROM {table} ORDER BY timestamp DESC LIMIT 1")
    result = cursor.fetchone()
    cursor.close()
    return datetime.fromtimestamp(result[0], tz=timezone.utc)


def get_last_metric(conn, table, metric):
    cursor = conn.cursor()
    cursor.execute(f"SELECT {metric} FROM {table} ORDER BY timestamp DESC LIMIT 1")
    result = cursor.fetchone()
    cursor.close()
    return result[0]


def get_all_metric(conn, table, metric):
    cursor = conn.cursor()
    cursor.execute(f"SELECT {metric} FROM {table} ORDER BY timestamp DESC")
    result = cursor.fetchall()
    cursor.close()
    return [row[0] for row in result]


def dates_to_query(query_date):
    query_dates = [query_date.date().isoformat()]
    while (
        query_date.date().isoformat() != datetime.now(timezone.utc).date().isoformat()
    ):
        query_date += timedelta(days=1)
        query_dates.append(query_date.date().isoformat())
    return query_dates


def get_buckets_detailed(current, bucket_width):
    buckets = []
    while current <= datetime.now(timezone.utc):
        current += bucket_width
        buckets.append(
            {
                "timestamp": current.timestamp(),
                "duration_s": bucket_width.total_seconds(),
                "start_time": current - bucket_width,
                "end_time": current,
            }
        )
    return buckets


def get_predominant_state(bucket_start, bucket_end, state_data, last_state):
    changes_before_start = [
        row for row in state_data if row["last_changed"] < bucket_end
    ]
    changes_in_bucket = [
        row for row in state_data if bucket_start < row["last_changed"] < bucket_end
    ]
    if len(changes_before_start) + len(changes_in_bucket) == 0:
        # no state changes before bucket_end, use last_state
        return last_state
    if len(changes_in_bucket) == 0:
        # no state changes between bucket_start and bucket_end, use previous state
        return changes_before_start[-1]["state"]
    # start counting time
    time_map = {
        changes_before_start[-1]["state"]: changes_in_bucket[0]["last_changed"]
        - bucket_start
    }
    for row in changes_in_bucket:
        if changes_in_bucket.index(row) + 1 == len(changes_in_bucket):
            # last in list, duration extends to end of bucket
            duration = bucket_end - row["last_changed"]
        else:
            # not last, duration extends to next change
            duration = (
                changes_in_bucket[changes_in_bucket.index(row) + 1]["last_changed"]
                - row["last_changed"]
            )
        # add to counter
        if not row["state"] in time_map:
            time_map.update({row["state"]: duration})
        else:
            time_map[row["state"]] += duration
    return max(time_map, key=time_map.get)


def convert_changelist_to_statelist(
    start_time, bucket_width, state_data, last_state, column_name
):
    data_submit = []
    buckets = get_buckets_detailed(start_time, bucket_width)
    for bucket in buckets:
        data_submit.append(
            {
                "timestamp": bucket["timestamp"],
                column_name: get_predominant_state(
                    bucket["start_time"], bucket["end_time"], state_data, last_state
                ),
            }
        )
    return data_submit


def insert_data(conn, table, data):
    print("Inserting", len(data), "rows...")
    cursor = conn.cursor()
    insert_statement = f"INSERT INTO {table} (%s) VALUES %s"
    for row in data:
        cursor.execute(
            insert_statement, (AsIs(",".join(row.keys())), tuple(row.values()))
        )
    conn.commit()
    cursor.close()
    return


def update_activity(conn):
    # configuration options
    table_name = "personal_activity"
    sensors = ["sensor.justin_pixel5_detected_activity"]
    acceptable_states = ["still", "walking", "running", "in_vehicle"]

    # get last row in table
    cursor = conn.cursor()
    last_time = get_last_timestamp(conn, table_name)
    last_state = get_last_metric(conn, table_name, "android_detected_activity")
    print("Updating table:", table_name)

    # get data from api
    response = get_data_hass(last_time, sensors)

    # collate data to submit
    data_submit = []
    for row in response[0]:
        if row["state"] in acceptable_states:
            current_state = row["state"]
            if current_state != last_state:
                data_submit.append(
                    {
                        "timestamp": datetime.fromisoformat(
                            row["last_changed"]
                        ).timestamp(),
                        "android_detected_activity": current_state,
                    }
                )
                last_state = current_state

    # push data to database
    insert_data(conn, table_name, data_submit)
    return


def update_location(conn):
    # configuration options
    table_name = "location"
    sensors = ["person.justin"]

    # get last row in table
    last_time = get_last_timestamp(conn, "location")
    last_state = get_last_metric(conn, table_name, "hass_detected_zone")
    print("Updating table:", table_name)

    # get data from api
    response = get_data_hass(last_time, sensors)

    # collate data to submit
    data_prep = [
        {
            "timestamp": datetime.fromisoformat(row["last_changed"]).timestamp(),
            "hass_detected_zone": row["state"],
        }
        for row in response[0]
    ]
    data_submit = [{"hass_detected_zone": last_state}]
    for row in data_prep:
        if row["hass_detected_zone"] != data_submit[-1]["hass_detected_zone"]:
            data_submit.append(row)
    data_submit.pop(0)

    # push data to database
    insert_data(conn, table_name, data_submit)
    return


def update_device_active(conn):
    # configuration options
    table_name = "device_active"
    bucket_width = 300  # seconds
    metric_data = {
        "pixel_screen_on": {
            "hass_entity_id": "binary_sensor.justin_pixel5_device_locked",
            "value_map": {
                "off": 1,  # off = unlocked
                "on": 0,  # on = locked
            },
        },
        "office_door_open": {
            "hass_entity_id": "binary_sensor.office_door",
            "value_map": {
                "off": 1,  # off = open
                "on": 0,  # on = closed
            },
        },
        "fireplace_tv_on": {
            "hass_entity_id": "media_player.fireplace_tv",
            "value_map": {
                "unavailable": 0,
                "idle": 0,
                "standby": 0,
                "on": 1,
                "paused": 1,
                "playing": 1,
            },
        },
    }

    # get last row in table
    last_time = get_last_timestamp(conn, table_name)
    print("Updating table:", table_name)

    # get data from api
    response = get_data_hass(
        last_time, [metric["hass_entity_id"] for metric in metric_data.values()]
    )
    response_list = [item for sublist in response for item in sublist]

    # build time buckets
    dt = last_time.timestamp() + bucket_width
    data_submit = []
    while dt < datetime.now(timezone.utc).timestamp():
        data_submit.append(
            {
                "timestamp": dt,
                "pixel_screen_on": 0,
                "office_door_open": 0,
                "fireplace_tv_on": 0,
            }
        )
        dt += bucket_width
    if len(data_submit) < 2:
        print("Skipping", table_name, "due to no new data.")
        return

    # run through entity metrics
    for metric in metric_data:
        metric_items = [
            row
            for row in response_list
            if row["entity_id"] == metric_data[metric]["hass_entity_id"]
        ]
        for row in metric_items:
            # get previous item's data
            if metric_items.index(row) == 0:
                # first item in dataset, use initial values
                previous_state = 1 - metric_data[metric]["value_map"][row["state"]]
                previous_timestamp = last_time.timestamp()
                previous_bucket_index = 0
                previous_time_until_bucket_end = bucket_width
            else:
                # use previous item's values
                previous_state = current_state
                previous_timestamp = current_timestamp
                previous_bucket_index = current_bucket_index
                previous_time_until_bucket_end = time_until_bucket_end

            # get this item's data
            current_state = metric_data[metric]["value_map"][row["state"]]
            current_timestamp = datetime.fromisoformat(row["last_changed"]).timestamp()
            for i, bucket in enumerate(data_submit):
                if current_timestamp <= bucket["timestamp"]:
                    current_bucket_index = i
                    break

            # get bucket data
            current_bucket_end = data_submit[current_bucket_index]["timestamp"]
            current_bucket_start = current_bucket_end - bucket_width

            # get differentials
            time_since_last_item = current_timestamp - previous_timestamp
            time_since_bucket_start = current_timestamp - current_bucket_start
            time_until_bucket_end = current_bucket_end - current_timestamp

            # fill from all datapoints
            if current_bucket_index == previous_bucket_index:
                # this item is in the same bucket as the previous one
                # add active duration to current bucket
                data_submit[current_bucket_index][metric] += (
                    time_since_last_item * previous_state / bucket_width
                )
            else:
                # this is the first item in the current bucket
                # add active duration to fill the last incomplete bucket
                data_submit[previous_bucket_index][metric] += (
                    previous_time_until_bucket_end * previous_state / bucket_width
                )
                # fill all buckets inbetween, if any
                for i in range(previous_bucket_index + 1, current_bucket_index):
                    data_submit[i][metric] += previous_state
                # add active duration to current bucket
                data_submit[current_bucket_index][metric] += (
                    time_since_bucket_start * previous_state / bucket_width
                )

        # finish the last incomplete bucket
        data_submit[current_bucket_index][metric] += (
            time_until_bucket_end * current_state / bucket_width
        )
        # fill any remaining buckets
        for i in range(current_bucket_index + 1, len(data_submit)):
            data_submit[i][metric] += current_state

    # push data to database
    insert_data(conn, table_name, data_submit)
    return


def update_heart_rate(conn):
    # configuration options
    table_name = "heart_rate"
    detail_level = "1min"

    # get last entry timestamp
    last_time = get_last_timestamp(conn, table_name)
    print("Updating table:", table_name)

    # get data from api
    data_submit = []
    for query_date in dates_to_query(last_time):
        query_url = (
            "https://api.fitbit.com/1/user/-/activities/heart/date/"
            + query_date
            + "/1d/"
            + detail_level
            + ".json"
        )
        query_response = get_data_fitbit(query_url)
        for row in query_response["activities-heart-intraday"]["dataset"]:
            # get timestamps
            if len(data_submit):
                prev_time = row_time
            else:
                prev_time = last_time
            row_time = datetime.fromisoformat(query_date + "T" + row["time"] + "Z")
            # check for overlaps and gaps
            if row_time < last_time:
                continue
            if row_time > prev_time + timedelta(minutes=1):
                minutes_missing = int((row_time - last_time).total_seconds() / 60)
                for i in range(1, minutes_missing - 1):
                    data_submit.append(
                        {
                            "timestamp": (last_time + timedelta(minutes=i)).timestamp(),
                            "heart_rate_bpm": None,
                        }
                    )
            data_submit.append(
                {
                    "timestamp": row_time.timestamp(),
                    "heart_rate_bpm": row["value"],
                }
            )

    # push data to database
    insert_data(conn, table_name, data_submit)
    return


def update_steps(conn):
    # configuration options
    table_name = "steps"
    detail_level = "1min"

    # get last entry timestamp
    last_time = get_last_timestamp(conn, table_name)
    print("Updating table:", table_name)

    # get data from api
    data_submit = []
    for query_date in dates_to_query(last_time):
        query_response = get_data_fitbit(
            "https://api.fitbit.com/1/user/-/activities/steps/date/"
            + query_date
            + "/1d/"
            + detail_level
            + ".json"
        )
        for row in query_response["activities-steps-intraday"]["dataset"]:
            # get timestamps
            if len(data_submit):
                prev_time = row_time
            else:
                prev_time = last_time
            row_time = datetime.fromisoformat(query_date + "T" + row["time"] + "Z")
            # check for overlaps and gaps
            if row_time < last_time or row_time > datetime.now(timezone.utc):
                continue
            if row_time > prev_time + timedelta(minutes=1):
                minutes_missing = int((row_time - last_time).total_seconds() / 60)
                for i in range(1, minutes_missing - 1):
                    data_submit.append(
                        {
                            "timestamp": last_time + timedelta(minutes=i),
                            "steps_count": None,
                        }
                    )
            data_submit.append(
                {
                    "timestamp": row_time.timestamp(),
                    "steps_count": row["value"],
                }
            )

    # push data to database
    insert_data(conn, table_name, data_submit)
    return


def update_sleep(conn):
    # configuration options
    table_name = "sleep"

    # get last entry timestamp
    last_time = get_last_timestamp(conn, table_name)
    sleep_ids = get_all_metric(conn, table_name, "fitbit_log_id")
    if datetime.now(timezone.utc) <= last_time + timedelta(hours=30):
        print("Skipping", table_name, "based on cooldown.")
        return
    else:
        print("Updating table:", table_name)

    # get data from api
    data_submit = []
    for query_date in dates_to_query(last_time):
        query_response = get_data_fitbit(
            "https://api.fitbit.com/1.2/user/-/sleep/date/" + query_date + ".json"
        )
        for row in query_response["sleep"]:
            if row["logId"] not in sleep_ids:
                data_submit.append(
                    {
                        "timestamp": datetime.fromisoformat(
                            row["endTime"] + "Z"
                        ).timestamp(),
                        "sleep_hours_inbed": row["timeInBed"] / 60,
                        "sleep_hours_asleep": row["minutesAsleep"] / 60,
                        "sleep_hours_deep": row["levels"]["summary"]["deep"]["minutes"]
                        / 60,
                        "sleep_hours_light": row["levels"]["summary"]["light"][
                            "minutes"
                        ]
                        / 60,
                        "sleep_hours_rem": row["levels"]["summary"]["rem"]["minutes"]
                        / 60,
                        "sleep_hours_wake": row["levels"]["summary"]["wake"]["minutes"]
                        / 60,
                        "sleep_time_start": datetime.fromisoformat(
                            row["startTime"] + "Z"
                        ),
                        "sleep_time_end": datetime.fromisoformat(row["endTime"] + "Z"),
                        "fitbit_log_id": row["logId"],
                    }
                )

    # push data to database
    insert_data(conn, table_name, data_submit)
    return


def main():
    # connect to database
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOSTNAME"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        database=os.environ.get("POSTGRES_DB", "sandiego"),
        user=os.environ.get("POSTGRES_USERNAME", "sandiego"),
        password=os.environ.get("POSTGRES_PASSWORD"),
    )

    # process each table
    update_activity(conn)
    update_location(conn)
    update_device_active(conn)
    update_heart_rate(conn)
    update_steps(conn)
    update_sleep(conn)

    # close the connection
    conn.close()
    print("All tables updated. Sleeping...")
    return


if __name__ == "__main__":
    print("App started.")
    while True:
        main()
        time.sleep(3660)
