from datetime import datetime, timedelta, timezone
import json
import csv
import glob
import os

json_files = glob.glob(os.path.join('./data/Takeout/Fit/All Sessions', '*SLEEP.json'))
data_submit = []
for json_file in json_files:
    with open(json_file) as file:
        data = json.load(file)
    time_start = datetime.fromisoformat(data['startTime'])
    time_end = datetime.fromisoformat(data['endTime'])
    data_submit.append({
        'timestamp': time_end.timestamp(),
        'sleep_hours_inbed': (time_end - time_start).total_seconds()/3600,
        'sleep_time_start': time_start,
        'sleep_time_end': time_end,
    })

existing_days = [row.date() for row in get_all_metric(conn, 'sleep', 'sleep_time_end')]
data_submit = [row for row in data_submit if not row['sleep_time_end'].date() in existing_days]