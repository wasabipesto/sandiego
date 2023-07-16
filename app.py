import json
import os
import time
from datetime import datetime, timedelta, timezone

import numpy as np
import psycopg2
import requests
import yaml
from psycopg2.extensions import AsIs


def get_database_connection():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOSTNAME"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        database=os.environ.get("POSTGRES_DB", "sandiego"),
        user=os.environ.get("POSTGRES_USERNAME", "sandiego"),
        password=os.environ.get("POSTGRES_PASSWORD"),
    )


def query_hass(start_time, sensors):
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
            # "no_attributes": True,
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


def get_data_hass(start_time, metric_data):
    hass_metrics = list(
        {
            metric["hass_metric_id"]
            for metric in metric_data.values()
            if metric["source"] == "hass"
        }
    )
    data_homeassistant_raw = query_hass(start_time, hass_metrics)
    data_homeassistant_formatted = format_hass_timestamps(data_homeassistant_raw)
    data_homeassistant = {
        metric: data_homeassistant_formatted[hass_metrics.index(metric)]
        for metric in hass_metrics
    }
    return data_homeassistant


def query_fitbit(url):
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


def dates_to_query_fitbit(start_datetime):
    query_date = start_datetime.date()
    end_date = datetime.now(timezone.utc).date()
    query_dates = []
    while query_date <= end_date:
        query_dates.append(query_date.isoformat())
        query_date += timedelta(days=1)
    return query_dates


def get_data_fitbit(start_time, metric_data):
    fitbit_type_data = {
        "sleep": {
            "url_start": "https://api.fitbit.com/1.2/user/-/sleep/date/",
            "url_end": ".json",
        },
        "steps": {
            "url_start": "https://api.fitbit.com/1/user/-/activities/steps/date/",
            "url_end": "/1d/1min.json",
        },
        "heart": {
            "url_start": "https://api.fitbit.com/1/user/-/activities/heart/date/",
            "url_end": "/1d/1min.json",
        },
        "hrv": {
            "url_start": "https://api.fitbit.com/1/user/-/hrv/date/",
            "url_end": "/all.json",
        },
    }
    fitbit_types = list(
        {
            metric["fitbit_type"]
            for metric in metric_data.values()
            if metric["source"] == "fitbit"
        }
    )

    query_dates = dates_to_query_fitbit(start_time)
    print("Fitbit API calls:", len(fitbit_types) * len(query_dates))
    # print("Requesting metrics:", fitbit_types)
    # print("Requesting dates:", query_dates)

    data_fitbit = {}
    for fitbit_type in fitbit_types:
        data_fitbit[fitbit_type] = {}
        for query_date_str in query_dates:
            url = (
                fitbit_type_data[fitbit_type]["url_start"]
                + query_date_str
                + fitbit_type_data[fitbit_type]["url_end"]
            )
            response = query_fitbit(url)
            data_fitbit[fitbit_type].update({query_date_str: response})
    return data_fitbit


def get_last_updated(conn, tables):
    cursor = conn.cursor()
    times = []
    for table in tables:
        cursor.execute(f"SELECT end_time FROM {table} ORDER BY end_time DESC LIMIT 1")
        result = cursor.fetchone()
        times.append(result[0])
    cursor.close()
    return min(times).astimezone(timezone.utc)


def get_last_row(conn, table):
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table} ORDER BY end_time DESC LIMIT 1")
    result = cursor.fetchone()
    cursor.close()
    column_names = [desc[0] for desc in cursor.description]
    row_dict = {column_names[i]: result[i] for i in range(len(column_names))}
    return row_dict


def get_buckets(bucket_time, bucket_width):
    buckets = {}
    bucket_time += bucket_width
    while bucket_time <= datetime.now(timezone.utc):
        buckets[bucket_time] = {
            "start_time": bucket_time - bucket_width,
            "end_time": bucket_time,
        }
        bucket_time += bucket_width
    return buckets


def state_data_to_bucket_durations(bucket_start, bucket_end, state_data, last_state):
    changes_before_start = [
        row for row in state_data if row["last_changed"] < bucket_end
    ]
    changes_in_bucket = [
        row for row in state_data if bucket_start < row["last_changed"] < bucket_end
    ]
    if len(changes_before_start) + len(changes_in_bucket) == 0:
        # no state changes before bucket_end, use last_state
        return {last_state: bucket_end - bucket_start}
    if len(changes_in_bucket) == 0:
        # no state changes between bucket_start and bucket_end, use previous state
        return {changes_before_start[-1]["state"]: bucket_end - bucket_start}
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
    return time_map


def get_predominant_state(bucket_start, bucket_end, state_data, last_state):
    time_map = state_data_to_bucket_durations(
        bucket_start, bucket_end, state_data, last_state
    )
    return max(time_map, key=time_map.get)


def get_state_duration_hours(
    bucket_start, bucket_end, state_data, last_state, select_states
):
    time_map = state_data_to_bucket_durations(
        bucket_start, bucket_end, state_data, last_state
    )
    duration_sum = sum(
        [
            time_map[state].total_seconds()
            for state in time_map
            if state in select_states
        ]
    )
    return duration_sum / 3600


def get_fitbit_steps_sum(bucket_start, bucket_end, fitbit_response):
    steps_list = []
    for query_date in fitbit_response:
        for row in fitbit_response[query_date]["activities-steps-intraday"]["dataset"]:
            row_time = datetime.fromisoformat(query_date + "T" + row["time"] + "Z")
            if bucket_start < row_time <= bucket_end:
                steps_list.append(row["value"])
    return sum(steps_list)


def get_fitbit_heart_mean(bucket_start, bucket_end, fitbit_response):
    hr_list = []
    for query_date in fitbit_response:
        for row in fitbit_response[query_date]["activities-heart-intraday"]["dataset"]:
            row_time = datetime.fromisoformat(query_date + "T" + row["time"] + "Z")
            if bucket_start < row_time <= bucket_end:
                hr_list.append(row["value"])
    if len(hr_list):
        return np.mean(hr_list)
    else:
        return None


def get_fitbit_heart_percentile(bucket_start, bucket_end, fitbit_response, percentile):
    hr_list = []
    for query_date in fitbit_response:
        for row in fitbit_response[query_date]["activities-heart-intraday"]["dataset"]:
            row_time = datetime.fromisoformat(query_date + "T" + row["time"] + "Z")
            if bucket_start < row_time <= bucket_end:
                hr_list.append(row["value"])
    if len(hr_list):
        return np.percentile(hr_list, percentile)
    else:
        return None


def get_fitbit_sleep(bucket_start, bucket_end, fitbit_response, fitbit_sleep_item):
    for query_date in fitbit_response:
        for row in fitbit_response[query_date]["sleep"]:
            row_time = datetime.fromisoformat(row["endTime"] + "Z")
            if bucket_start < row_time <= bucket_end:
                if fitbit_sleep_item == "hours_inbed":
                    return row["timeInBed"] / 60
                elif fitbit_sleep_item == "hours_asleep":
                    return row["minutesAsleep"] / 60
                elif fitbit_sleep_item == "hours_deep":
                    return row["levels"]["summary"]["deep"]["minutes"] / 60
                elif fitbit_sleep_item == "hours_light":
                    return row["levels"]["summary"]["light"]["minutes"] / 60
                elif fitbit_sleep_item == "hours_rem":
                    return row["levels"]["summary"]["rem"]["minutes"] / 60
                elif fitbit_sleep_item == "hours_wake":
                    return row["levels"]["summary"]["wake"]["minutes"] / 60
                elif fitbit_sleep_item == "time_start":
                    return datetime.fromisoformat(row["startTime"] + "Z")
                elif fitbit_sleep_item == "time_end":
                    return datetime.fromisoformat(row["endTime"] + "Z")
                raise Exception("Sleep property not supported.")
    print("Warning: No sleep entry found in range.")
    return None


def get_fitbit_isasleep(bucket_start, bucket_end, fitbit_response):
    return False  # TODO


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


def main():
    print("Starting new run at", datetime.now(tz=timezone.utc).isoformat())

    conn = get_database_connection()

    with open("configuration.yml", "r") as file:
        configuration_data = yaml.safe_load(file)
    table_data = configuration_data["tables"]
    metric_data = configuration_data["metrics"]

    # get last updated time
    last_updated = get_last_updated(conn, table_data.keys())
    print("Last updated at", last_updated.isoformat())

    print("Downloading metrics from Home Assistant...")
    data_homeassistant = get_data_hass(last_updated, metric_data)

    print("Downloading metrics from Fitbit...")
    data_fitbit = get_data_fitbit(last_updated, metric_data)

    for table_name in table_data:
        print("Processing table", table_name, "...")
        last_row = get_last_row(conn, table_name)
        submit_data = get_buckets(
            last_row["end_time"].astimezone(timezone.utc),
            timedelta(minutes=table_data[table_name]["duration_minutes"]),
        )
        for metric_name in metric_data:
            metric_specs = metric_data[metric_name]
            if table_name in metric_specs["tables"]:
                print("Processing metric", metric_name, "...")
                for end_time in submit_data:
                    row = submit_data[end_time]
                    # aggregate methods
                    if metric_specs["aggregate"] == "fitbit_sleep":
                        value = get_fitbit_sleep(
                            row["start_time"],
                            row["end_time"],
                            data_fitbit[metric_specs["fitbit_type"]],
                            metric_specs["fitbit_sleep_item"],
                        )
                    elif metric_specs["aggregate"] == "fitbit_steps_sum":
                        value = get_fitbit_steps_sum(
                            row["start_time"],
                            row["end_time"],
                            data_fitbit[metric_specs["fitbit_type"]],
                        )
                    elif metric_specs["aggregate"] == "fitbit_heart_mean":
                        value = get_fitbit_heart_mean(
                            row["start_time"],
                            row["end_time"],
                            data_fitbit[metric_specs["fitbit_type"]],
                        )
                    elif metric_specs["aggregate"] == "fitbit_heart_percentile":
                        value = get_fitbit_heart_percentile(
                            row["start_time"],
                            row["end_time"],
                            data_fitbit[metric_specs["fitbit_type"]],
                            metric_specs["fitbit_heart_percentile"],
                        )
                    elif metric_specs["aggregate"] == "hass_state_to_select":
                        value = get_predominant_state(
                            row["start_time"],
                            row["end_time"],
                            data_homeassistant[metric_specs["hass_metric_id"]],
                            last_row[metric_name],
                        )
                    elif metric_specs["aggregate"] == "hass_state_to_hours":
                        value = get_state_duration_hours(
                            row["start_time"],
                            row["end_time"],
                            data_homeassistant[metric_specs["hass_metric_id"]],
                            last_row[metric_name],
                            metric_specs["select_states"],
                        )
                    submit_data[end_time].update({metric_name: value})
        insert_data(conn, table_name, submit_data.values())

    # close the connection
    conn.close()
    print("All tables updated at", datetime.now(tz=timezone.utc).isoformat())
    print("Sleeping for 15 minutes.")
    return


if __name__ == "__main__":
    print("App started.")
    while True:
        main()
        time.sleep(15 * 60)
