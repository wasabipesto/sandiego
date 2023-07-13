from datetime import datetime, timedelta, timezone
import json
import csv
import glob
import os

buckets = [
    {
        'start': datetime(2022,12,1, tzinfo=timezone.utc),
        'end': datetime(2023,1,1, tzinfo=timezone.utc),
    },{
        'start': datetime(2023,1,1, tzinfo=timezone.utc),
        'end': datetime(2023,2,1, tzinfo=timezone.utc),
    },{
        'start': datetime(2023,2,1, tzinfo=timezone.utc),
        'end': datetime(2023,3,1, tzinfo=timezone.utc),
    },{
        'start': datetime(2023,3,1, tzinfo=timezone.utc),
        'end': datetime(2023,4,1, tzinfo=timezone.utc),
    },{
        'start': datetime(2023,4,1, tzinfo=timezone.utc),
        'end': datetime(2023,5,1, tzinfo=timezone.utc),
    },{
        'start': datetime(2023,5,1, tzinfo=timezone.utc),
        'end': datetime(2023,6,1, tzinfo=timezone.utc),
    },{
        'start': datetime(2023,6,1, tzinfo=timezone.utc),
        'end': datetime(2023,7,1, tzinfo=timezone.utc),
    },{
        'start': datetime(2023,7,1, tzinfo=timezone.utc),
        'end': datetime(2023,7,4, tzinfo=timezone.utc),
    },
]

longest_entries = {}
for bucket in buckets:
    response = requests.get(
        'http://prometheus:9090/api/v1/query_range',
        params={
            'query': 'hassio_binary_sensor_state{friendly_name="Justin Asleep"}',
            'start': bucket['start'].timestamp(),
            'end': bucket['end'].timestamp(),
            'step': 300,
        }
        ).json()

    current_state = False
    data_review = []
    for row in response['data']['result'][0]['values']:
        time = datetime.fromtimestamp(row[0], tz=timezone.utc)
        prev_state = current_state
        if row[1] == '1':
            current_state = True
        elif row[1] == '0':
            current_state = False
        else:
            raise Exception('state not defined')
        if current_state != prev_state:
            if current_state == True:
                fell_asleep = time
            else:
                data_review.append({
                    'timestamp': time.timestamp(),
                    'sleep_hours_inbed': (time - fell_asleep).total_seconds()/3600,
                    'sleep_time_start': fell_asleep,
                    'sleep_time_end': time,
                })

    for entry in data_review:
        date = entry['sleep_time_end'].date()
        if date not in longest_entries:
            longest_entries[date] = entry
        else:
            if entry['sleep_hours_inbed'] > longest_entries[date]['sleep_hours_inbed']:
                longest_entries[date] = entry
data_submit = list(longest_entries.values())
