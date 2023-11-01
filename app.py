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


def query_hass(query_start, sensors):
    sensors_string = ",".join([s for s in sensors])
    response = requests.get(
        os.environ.get("HASS_URL") + "/api/history/period/" + query_start.isoformat(),
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


def get_data_hass(query_start, metric_data):
    print("Downloading metrics from Home Assistant...")
    hass_metrics = list(
        {
            metric["hass_metric_id"]
            for metric in metric_data.values()
            if metric["provider"] == "homeassistant"
        }
    )
    if len(hass_metrics):
        data_homeassistant_raw = query_hass(query_start, hass_metrics)
        data_homeassistant_formatted = format_hass_timestamps(data_homeassistant_raw)
        if len(hass_metrics) != len(data_homeassistant_formatted):
            print("Not enough records returned from Home Assistant!")
        data_homeassistant = {
            metric: data_homeassistant_formatted[hass_metrics.index(metric)]
            for metric in hass_metrics
        }
        return data_homeassistant
    else:
        return {}


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

    elif response.status_code == 429:
        raise Exception("Fitbit API rate-limited.", response.headers)

    return response.json()


def dates_to_query_fitbit(query_start, query_end):
    query_date = query_start.date()
    end_date = query_end.date()
    query_dates = []
    while query_date <= end_date:
        query_dates.append(query_date.isoformat())
        query_date += timedelta(days=1)
    return query_dates


def get_data_fitbit(query_start, query_end, url_schemas, metric_config):
    fitbit_types = list(
        {
            metric["fitbit_type"]
            for metric in metric_config.values()
            if metric["provider"] == "fitbit"
        }
    )

    query_dates = dates_to_query_fitbit(query_start, query_end)
    print(
        "Downloading metrics from Fitbit:",
        len(fitbit_types) * len(query_dates),
        "calls...",
    )
    # print("Requesting metrics:", fitbit_types)
    # print("Requesting dates:", query_dates)

    data_fitbit = {}
    for fitbit_type in fitbit_types:
        data_fitbit[fitbit_type] = {}
        for query_date_str in query_dates:
            url = (
                url_schemas[fitbit_type]["url_start"]
                + query_date_str
                + url_schemas[fitbit_type]["url_end"]
            )
            response = query_fitbit(url)
            data_fitbit[fitbit_type].update({query_date_str: response})
    return data_fitbit


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
    elif len(changes_in_bucket) == 0:
        # no state changes between bucket_start and bucket_end, use previous state
        return {changes_before_start[-1]["state"]: bucket_end - bucket_start}
    else:
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


def get_state_sensor_analog(
    bucket_start, bucket_end, state_data, last_state, analog_aggregate_method
):
    time_map = state_data_to_bucket_durations(
        bucket_start, bucket_end, state_data, last_state
    )
    time_map.pop(None, None)
    time_map.pop("unavailable", None)
    if len(time_map) == 0:
        return None
    elif analog_aggregate_method == "mean":
        return sum(
            [float(key) * value.total_seconds() for key, value in time_map.items()]
        ) / sum([value.total_seconds() for key, value in time_map.items()])
    elif analog_aggregate_method == "minimum":
        return float(min(time_map.keys()))
    elif analog_aggregate_method == "maximum":
        return float(max(time_map.keys()))
    else:
        raise Exception("Aggregate method not supported.")


def get_fitbit_steps_sum(bucket_start, bucket_end, data_fitbit):
    steps_list = []
    for query_date in data_fitbit:
        for row in data_fitbit[query_date]["activities-steps-intraday"]["dataset"]:
            row_time = datetime.fromisoformat(query_date + "T" + row["time"] + "Z")
            if bucket_start < row_time <= bucket_end:
                steps_list.append(row["value"])
    return sum(steps_list)


def get_fitbit_heart_mean(bucket_start, bucket_end, data_fitbit):
    hr_list = []
    for query_date in data_fitbit:
        for row in data_fitbit[query_date]["activities-heart-intraday"]["dataset"]:
            row_time = datetime.fromisoformat(query_date + "T" + row["time"] + "Z")
            if bucket_start < row_time <= bucket_end:
                hr_list.append(row["value"])
    if len(hr_list):
        return np.mean(hr_list)
    else:
        return None


def get_fitbit_heart_percentile(bucket_start, bucket_end, data_fitbit, percentile):
    hr_list = []
    for query_date in data_fitbit:
        for row in data_fitbit[query_date]["activities-heart-intraday"]["dataset"]:
            row_time = datetime.fromisoformat(query_date + "T" + row["time"] + "Z")
            if bucket_start < row_time <= bucket_end:
                hr_list.append(row["value"])
    if len(hr_list):
        return np.percentile(hr_list, percentile)
    else:
        return None


def get_fitbit_heart_rmssd(bucket_start, bucket_end, data_fitbit):
    try:
        return data_fitbit[bucket_start.date().isoformat()]["hrv"][0]["value"][
            "dailyRmssd"
        ]
    except IndexError:
        print("Warning: No HRV entry found for date", bucket_start.date().isoformat())
        return None


def get_fitbit_sleep(bucket_start, bucket_end, data_fitbit, fitbit_sleep_item):
    for query_date, query_data in data_fitbit.items():
        if query_date == bucket_start.date().isoformat() and len(query_data["sleep"]):
            sleep_item = query_data["sleep"][0]  # TODO: Get the longest sleep item
            if fitbit_sleep_item == "hours_inbed":
                return sleep_item["timeInBed"] / 60
            elif fitbit_sleep_item == "hours_asleep":
                return sleep_item["minutesAsleep"] / 60
            elif fitbit_sleep_item == "hours_deep":
                return sleep_item["levels"]["summary"]["deep"]["minutes"] / 60
            elif fitbit_sleep_item == "hours_light":
                return sleep_item["levels"]["summary"]["light"]["minutes"] / 60
            elif fitbit_sleep_item == "hours_rem":
                return sleep_item["levels"]["summary"]["rem"]["minutes"] / 60
            elif fitbit_sleep_item == "hours_wake":
                return sleep_item["levels"]["summary"]["wake"]["minutes"] / 60
            elif fitbit_sleep_item == "time_start":
                return datetime.fromisoformat(sleep_item["startTime"] + "Z")
            elif fitbit_sleep_item == "time_end":
                return datetime.fromisoformat(sleep_item["endTime"] + "Z")
            raise Exception("Sleep property not supported.")
    print("Warning: No sleep entry found for date", bucket_start.date().isoformat())
    return None


def get_buckets(query_start, query_end, bucket_width, align_offset):
    buckets = []
    bucket_time = (
        query_start.replace(hour=0, minute=0, second=0, microsecond=0) + align_offset
    )
    while bucket_time < query_start:
        bucket_time += bucket_width
    bucket_time += bucket_width
    while bucket_time <= query_end:
        buckets.append(
            {
                "start_time": bucket_time - bucket_width,
                "end_time": bucket_time,
            }
        )
        bucket_time += bucket_width
    return buckets


def insert_new_buckets(conn, table, data):
    cursor = conn.cursor()
    statement = f"INSERT INTO {table} (%s) VALUES %s ON CONFLICT DO NOTHING"
    for row in data:
        cursor.execute(statement, (AsIs(",".join(row.keys())), tuple(row.values())))
    conn.commit()
    cursor.close()
    return


def get_table_data(conn, table, query_start, query_end):
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT * FROM {table} WHERE start_time > %s AND end_time < %s ORDER BY end_time",
        (query_start.isoformat(), query_end.isoformat()),
    )
    result = cursor.fetchall()
    cursor.close()
    column_names = [desc[0] for desc in cursor.description]
    return [
        {column_names[i]: row[i] for i in range(len(column_names))} for row in result
    ]


def update_data(conn, table, column, data):
    # data = {key: value for key, value in data.items() if value is not None}
    print("Updating", len(data), "rows in", table, column)
    cursor = conn.cursor()
    statement = f"UPDATE {table} SET {column} = %s WHERE id = %s;"
    for key, value in data.items():
        cursor.execute(statement, (value, key))
    conn.commit()
    cursor.close()
    return


def main():
    print("Starting new run at", datetime.now().isoformat())

    conn = get_database_connection()

    with open("configuration.yml", "r") as file:
        configuration_data = yaml.safe_load(file)
    provider_config = configuration_data["providers"]
    table_config = configuration_data["tables"]
    metric_config = configuration_data["metrics"]

    backfill_metric = os.environ.get("SANDIEGO_BACKFILL_METRIC")
    if backfill_metric in metric_config.keys():
        metric_config = {backfill_metric: metric_config[backfill_metric]}

    lookback_minutes = int(os.environ.get("SANDIEGO_LOOKBACK_MINUTES", 2 * 24 * 60))
    lookback_duration = timedelta(minutes=lookback_minutes)
    query_end = datetime.now(timezone.utc)
    query_start = query_end - lookback_duration

    if provider_config["homeassistant"]["enabled"]:
        data_homeassistant = get_data_hass(query_start, metric_config)

    if provider_config["fitbit"]["enabled"]:
        data_fitbit = get_data_fitbit(
            query_start,
            query_end,
            provider_config["fitbit"]["url_schemas"],
            metric_config,
        )

    for table_name in table_config:
        # insert new rows
        new_buckets = get_buckets(
            query_start,
            query_end,
            timedelta(minutes=table_config[table_name]["duration_minutes"]),
            timedelta(minutes=table_config[table_name]["align_offset_minutes"]),
        )
        insert_new_buckets(conn, table_name, new_buckets)

        # get relevant metrics
        table_metrics = [
            metric
            for metric in metric_config
            if table_name in metric_config[metric]["tables"]
            and provider_config[metric_config[metric]["provider"]]["enabled"]
        ]

        # get current data
        table_data = get_table_data(conn, table_name, query_start, query_end)

        for metric_name in table_metrics:
            metric_specs = metric_config[metric_name]
            submit_data = {}
            for row in table_data:
                # aggregate methods
                if metric_specs["aggregate"] == "fitbit_sleep":
                    submit_data[row["id"]] = get_fitbit_sleep(
                        row["start_time"],
                        row["end_time"],
                        data_fitbit[metric_specs["fitbit_type"]],
                        metric_specs["fitbit_sleep_item"],
                    )
                elif metric_specs["aggregate"] == "fitbit_steps_sum":
                    submit_data[row["id"]] = get_fitbit_steps_sum(
                        row["start_time"],
                        row["end_time"],
                        data_fitbit[metric_specs["fitbit_type"]],
                    )
                elif metric_specs["aggregate"] == "fitbit_heart_mean":
                    submit_data[row["id"]] = get_fitbit_heart_mean(
                        row["start_time"],
                        row["end_time"],
                        data_fitbit[metric_specs["fitbit_type"]],
                    )
                elif metric_specs["aggregate"] == "fitbit_heart_percentile":
                    submit_data[row["id"]] = get_fitbit_heart_percentile(
                        row["start_time"],
                        row["end_time"],
                        data_fitbit[metric_specs["fitbit_type"]],
                        metric_specs["fitbit_heart_percentile"],
                    )
                elif metric_specs["aggregate"] == "fitbit_heart_rmssd":
                    submit_data[row["id"]] = get_fitbit_heart_rmssd(
                        row["start_time"],
                        row["end_time"],
                        data_fitbit[metric_specs["fitbit_type"]],
                    )
                elif metric_specs["aggregate"] == "hass_state_to_select":
                    submit_data[row["id"]] = get_predominant_state(
                        row["start_time"],
                        row["end_time"],
                        data_homeassistant[metric_specs["hass_metric_id"]],
                        table_data[-1][metric_name],
                    )
                elif metric_specs["aggregate"] == "hass_state_to_hours":
                    submit_data[row["id"]] = get_state_duration_hours(
                        row["start_time"],
                        row["end_time"],
                        data_homeassistant[metric_specs["hass_metric_id"]],
                        table_data[-1][metric_name],
                        metric_specs["select_states"],
                    )
                elif metric_specs["aggregate"] == "hass_state_sensor_analog":
                    submit_data[row["id"]] = get_state_sensor_analog(
                        row["start_time"],
                        row["end_time"],
                        data_homeassistant[metric_specs["hass_metric_id"]],
                        table_data[-1][metric_name],
                        metric_specs["analog_aggregate_method"],
                    )
            update_data(conn, table_name, metric_name, submit_data)

    # close the connection, clean up
    conn.close()
    print("All tables updated at", datetime.now().isoformat())
    return


if __name__ == "__main__":
    print("App started.")
    while True:
        main()
        sleep_minutes = int(os.environ.get("SANDIEGO_SLEEP_MINUTES", "15"))
        time.sleep(sleep_minutes * 60)
