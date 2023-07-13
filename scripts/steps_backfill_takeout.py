from datetime import datetime, timedelta, timezone
import json
import csv
import glob
import os

date = datetime(2020,4,5)
end_date = datetime(2023,7,4)
data_submit = []
while date < end_date:
    with open('./data/Takeout/Fit/Daily activity metrics/'+date.date().isoformat()+'.csv') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row['Step count']:
                data_submit.append({
                    'timestamp': datetime.fromisoformat(date.date().isoformat()+'T'+row['End time']).timestamp(),
                    'steps_count': int(row['Step count']),
                })
            else:
                data_submit.append({
                    'timestamp': datetime.fromisoformat(date.date().isoformat()+'T'+row['End time']).timestamp(),
                    'steps_count': 0,
                })
    date += timedelta(days=1)