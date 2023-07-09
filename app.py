import json
import os
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import requests


def get_data_fitbit(url):
    # load secrets file
    with open('./secrets/fitbit.json', 'r') as file:
        fitbit_secrets = json.load(file)

    # pull data from api
    response = requests.get(
        url, 
        headers={'authorization': 'Bearer '+fitbit_secrets['access_token']},
        params={'timezone': 'UTC'},
        )
    
    if response.status_code == 429:
        raise Exception('Fitbit API rate-limited.', response.headers)

    if response.status_code == 401:
        # request new key
        response = requests.post(
            'https://api.fitbit.com/oauth2/token',
            headers={
                'authorization': 'Basic '+fitbit_secrets['basic_token'],
                'content-type': 'application/x-www-form-urlencoded',
            },
            data={
                'grant_type': 'refresh_token',
                'client_id': fitbit_secrets['client_id'],
                'refresh_token': fitbit_secrets['refresh_token'],
            }).json()

        # update secrets file
        fitbit_secrets.update({'access_token':response['access_token'],'refresh_token':response['refresh_token']})
        with open('./secrets/fitbit.json', 'w') as file:
            json.dump(fitbit_secrets, file)

        # try again
        response = requests.get(
            url, 
            headers={'authorization': 'Bearer '+fitbit_secrets['access_token']},
            params={'timezone': 'UTC'},
            )
    return response.json()

def update_activity(conn):
    print('Updating table: activity')

    # get last row in table
    cursor = conn.cursor()
    cursor.execute('SELECT timestamp, android_detected_activity FROM personal_activity ORDER BY timestamp DESC LIMIT 1')
    result = cursor.fetchone()
    last_time = datetime.fromtimestamp(result[0], tz=timezone.utc)
    last_state = result[1]

    # get data from api
    response = requests.get(
        os.environ.get('HASS_URL')+'/api/history/period/'+last_time.isoformat(),
        headers={
            'authorization': 'Bearer '+os.environ.get('HASS_API_KEY'),
            'content-type': 'application/json',
        },
        params={
            'filter_entity_id': 'sensor.justin_pixel5_detected_activity',
            'end_time': datetime.now(timezone.utc).isoformat(),
            'no_attributes': True
        }
        ).json()
    
    # collate data to submit
    acceptable_states = ['still', 'walking', 'running', 'in_vehicle']
    data_prep = [{'time': datetime.fromisoformat(row['last_changed']), 'state': row['state']} for row in response[0] if row['state'] in acceptable_states]
    data_submit = [{'state':last_state}]
    for row in data_prep:
        if row['state'] != data_submit[-1]['state']:
            data_submit.append(row)
    data_submit.pop(0)

    if len(data_submit) == 0:
        cursor.close()
        return
    
    # push data to database
    print('Inserting', len(data_submit), 'rows...')
    for row in data_submit:
        cursor.execute('INSERT INTO personal_activity (timestamp, android_detected_activity) VALUES (%s, %s)',
            (row['time'].timestamp(), row['state'])
            )
    conn.commit()
    cursor.close()
    return

def update_location(conn):
    print('Updating table: location')

    # get last row in table
    cursor = conn.cursor()
    cursor.execute('SELECT timestamp, hass_detected_zone FROM location ORDER BY timestamp DESC LIMIT 1')
    result = cursor.fetchone()
    last_time = datetime.fromtimestamp(result[0], tz=timezone.utc)
    last_state = result[1]

    # get data from api
    response = requests.get(
        os.environ.get('HASS_URL')+'/api/history/period/'+last_time.isoformat(),
        headers={
            'authorization': 'Bearer '+os.environ.get('HASS_API_KEY'),
            'content-type': 'application/json',
        },
        params={
            'filter_entity_id': 'person.justin',
            'end_time': datetime.now(timezone.utc).isoformat(),
            'no_attributes': True
        }
        ).json()
    
    # collate data to submit
    data_prep = [{'time': datetime.fromisoformat(row['last_changed']), 'state': row['state']} for row in response[0]]
    data_submit = [{'state':last_state}]
    for row in data_prep:
        if row['state'] != data_submit[-1]['state']:
            data_submit.append(row)
    data_submit.pop(0)
    
    # push data to database
    print('Inserting', len(data_submit), 'rows...')
    for row in data_submit:
        cursor.execute('INSERT INTO location (timestamp, hass_detected_zone) VALUES (%s, %s)',
            (row['time'].timestamp(), row['state'])
            )
    conn.commit()
    cursor.close()
    return

def update_device_active(conn):
    print('Updating table: device_active')

    # configuration options
    bucket_width = timedelta(minutes=5)
    metric_data = {
        'pixel_screen_on': {
            'hass_entity_id': 'binary_sensor.justin_pixel5_device_locked',
            'value_map': {
                'off': 1, # off = unlocked
                'on': 0,  # on = locked
            }
        },
        'office_door_open': {
            'hass_entity_id': 'binary_sensor.office_door',
            'value_map': {
                'off': 1, # off = open
                'on': 0,  # on = closed
            }
        },
        'fireplace_tv_on': {
            'hass_entity_id': 'media_player.fireplace_tv',
            'value_map': {
                'unavailable': 0,
                'idle': 0,
                'standby': 0,
                'on': 1,
                'paused': 1,
                'playing': 1,
            }
        },
    }

    # get last row in table
    cursor = conn.cursor()
    cursor.execute('SELECT timestamp FROM device_active ORDER BY timestamp DESC LIMIT 1')
    result = cursor.fetchone()
    last_time = datetime.fromtimestamp(result[0], tz=timezone.utc)

    # get data from api
    response = requests.get(
        os.environ.get('HASS_URL')+'/api/history/period/'+last_time.isoformat(),
        headers={
            'authorization': 'Bearer '+os.environ.get('HASS_API_KEY'),
            'content-type': 'application/json',
        },
        params={
            'filter_entity_id': 
                'binary_sensor.justin_pixel5_device_locked,'+
                'binary_sensor.office_door,'+
                'media_player.fireplace_tv',
            'end_time': datetime.now(timezone.utc).isoformat(),
            'no_attributes': True
        }
        ).json()
    response_list = [item for sublist in response for item in sublist]

    # build time buckets
    dt = last_time + bucket_width
    buckets = []
    while dt < datetime.now(timezone.utc):
        buckets.append({
            'end_time': dt,
            'pixel_screen_on': 0,
            'office_door_open': 0,
            'fireplace_tv_on': 0,
            })
        dt += bucket_width
    if len(buckets) < 2:
        cursor.close()
        return

    # run through entity metrics
    for metric in metric_data:
        metric_items = [row for row in response_list if row['entity_id'] == metric_data[metric]['hass_entity_id']]
        for row in metric_items:
            # get previous item's data
            if metric_items.index(row) == 0:
                # first item in dataset, use initial values
                previous_state = 1 - metric_data[metric]['value_map'][row['state']]
                previous_timestamp = last_time
                previous_bucket_index = 0
                previous_time_until_bucket_end = bucket_width
            else:
                # use previous item's values
                previous_state = current_state
                previous_timestamp = current_timestamp
                previous_bucket_index = current_bucket_index
                previous_time_until_bucket_end = time_until_bucket_end
            
            # get this item's data
            current_state = metric_data[metric]['value_map'][row['state']]
            current_timestamp = datetime.fromisoformat(row['last_changed'])
            for i, bucket in enumerate(buckets):
                if current_timestamp <= bucket['end_time']:
                    current_bucket_index = i
                    break
            
            # get bucket data
            current_bucket_end = buckets[current_bucket_index]['end_time']
            current_bucket_start = current_bucket_end - bucket_width

            # get differentials
            time_since_last_item = current_timestamp - previous_timestamp
            time_since_bucket_start = current_timestamp - current_bucket_start
            time_until_bucket_end = current_bucket_end - current_timestamp

            # fill from all datapoints
            if current_bucket_index == previous_bucket_index:
                # this item is in the same bucket as the previous one
                # add active duration to current bucket
                #print(metric, 'bucket', current_bucket_index, 'intra +', time_since_last_item * previous_state / bucket_width)
                buckets[current_bucket_index][metric] += time_since_last_item * previous_state / bucket_width
            else:
                # this is the first item in the current bucket
                # add active duration to fill the last incomplete bucket
                #print(metric, 'bucket', previous_bucket_index, 'end   +', previous_time_until_bucket_end * previous_state / bucket_width)
                buckets[previous_bucket_index][metric] += previous_time_until_bucket_end * previous_state / bucket_width
                # fill all buckets inbetween, if any
                for i in range(previous_bucket_index+1,current_bucket_index):
                    #print(metric, 'bucket', i, 'fill  +', previous_state)
                    buckets[i][metric] += previous_state
                # add active duration to current bucket
                #print(metric, 'bucket', current_bucket_index, 'first +', time_since_bucket_start * previous_state / bucket_width)
                buckets[current_bucket_index][metric] += time_since_bucket_start * previous_state / bucket_width
            
        # finish the last incomplete bucket
        #print(metric, 'bucket', current_bucket_index, 'end   +', time_until_bucket_end * current_state / bucket_width)
        buckets[current_bucket_index][metric] += time_until_bucket_end * current_state / bucket_width
        # fill any remaining buckets
        for i in range(current_bucket_index+1,len(buckets)):
            #print(metric, 'bucket', i, 'fill  +', current_state)
            buckets[i][metric] += current_state
    data_submit = buckets
    
    # push data to database
    print('Inserting', len(data_submit), 'rows...')
    for row in data_submit:
        cursor.execute('INSERT INTO device_active (timestamp, pixel_screen_on, office_door_open, fireplace_tv_on) VALUES (%s, %s, %s, %s)',
            (row['end_time'].timestamp(), row['pixel_screen_on'], row['office_door_open'], row['fireplace_tv_on'])
            )
    conn.commit()
    cursor.close()
    return

def update_heart_rate(conn):
    print('Updating table: heart_rate')

    # configuration options
    detail_level = '1min'
    
    # get last entry timestamp
    cursor = conn.cursor()
    cursor.execute('SELECT timestamp FROM heart_rate ORDER BY timestamp DESC LIMIT 1')
    result = cursor.fetchone()
    last_time = datetime.fromtimestamp(result[0], tz=timezone.utc)

    # get list of dates to download
    # can only download granular stats in increments of 24h or less
    query_date = last_time
    query_dates = [query_date.date().isoformat()]
    while query_date.date().isoformat() != datetime.now(timezone.utc).date().isoformat():
        query_date += timedelta(days=1)
        query_dates.append(query_date.date().isoformat())
    
    # get data from api
    data_submit = []
    for query_date in query_dates:
        query_url = 'https://api.fitbit.com/1/user/-/activities/heart/date/'+query_date+'/1d/'+detail_level+'.json'
        query_response = get_data_fitbit(query_url)
        for row in query_response['activities-heart-intraday']['dataset']:
            # get timestamps
            if len(data_submit):
                prev_time = row_time
            else:
                prev_time = last_time
            row_time = datetime.fromisoformat(query_date+'T'+row['time']+'Z')
            # check for overlaps and gaps
            if row_time < last_time:
                continue
            if row_time > prev_time + timedelta(minutes=1):
                minutes_missing = int((row_time - last_time).total_seconds() / 60)
                for i in range(1, minutes_missing-1):
                    data_submit.append({
                        'timestamp': last_time+timedelta(minutes=i),
                        'heart_rate_bpm': None,
                    })
            data_submit.append({
                'timestamp': row_time,
                'heart_rate_bpm': row['value'],
            })
    
    # push data to database
    print('Inserting', len(data_submit), 'rows...')
    for row in data_submit:
        cursor.execute('INSERT INTO heart_rate (timestamp, heart_rate_bpm) VALUES (%s, %s)',
            (row['timestamp'].timestamp(), row['heart_rate_bpm'])
            )
    conn.commit()
    cursor.close()
    return

def update_steps(conn):
    print('Updating table: steps')

    # configuration options
    detail_level = '1min'
    
    # get last entry timestamp
    cursor = conn.cursor()
    cursor.execute('SELECT timestamp FROM steps ORDER BY timestamp DESC LIMIT 1')
    result = cursor.fetchone()
    last_time = datetime.fromtimestamp(result[0], tz=timezone.utc)

    # get list of dates to download
    # can only download granular stats in increments of 24h or less
    query_date = last_time
    query_dates = [query_date.date().isoformat()]
    while query_date.date().isoformat() != datetime.now(timezone.utc).date().isoformat():
        query_date += timedelta(days=1)
        query_dates.append(query_date.date().isoformat())
    
    # get data from api
    data_submit = []
    for query_date in query_dates:
        query_url = 'https://api.fitbit.com/1/user/-/activities/steps/date/'+query_date+'/1d/'+detail_level+'.json'
        query_response = get_data_fitbit(query_url)
        for row in query_response['activities-steps-intraday']['dataset']:
            # get timestamps
            if len(data_submit):
                prev_time = row_time
            else:
                prev_time = last_time
            row_time = datetime.fromisoformat(query_date+'T'+row['time']+'Z')
            # check for overlaps and gaps
            if row_time < last_time:
                continue
            if row_time > prev_time + timedelta(minutes=1):
                minutes_missing = int((row_time - last_time).total_seconds() / 60)
                for i in range(1, minutes_missing-1):
                    data_submit.append({
                        'timestamp': last_time+timedelta(minutes=i),
                        'steps_count': None,
                    })
            data_submit.append({
                'timestamp': row_time,
                'steps_count': row['value'],
            })
    
    # push data to database
    print('Inserting', len(data_submit), 'rows...')
    for row in data_submit:
        cursor.execute('INSERT INTO steps (timestamp, steps_count) VALUES (%s, %s)',
            (row['timestamp'].timestamp(), row['steps_count'])
            )
    conn.commit()
    cursor.close()
    return

def main():
    # connect to database
    conn = psycopg2.connect(
        host=os.environ.get('POSTGRES_HOSTNAME'),
        port=os.environ.get('POSTGRES_PORT', '5432'),
        database=os.environ.get('POSTGRES_DB', 'sandiego'),
        user=os.environ.get('POSTGRES_USERNAME', 'sandiego'),
        password=os.environ.get('POSTGRES_PASSWORD')
    )

    # process each table
    update_activity(conn)
    update_location(conn)
    update_device_active(conn)
    update_heart_rate(conn)
    update_steps(conn)

    # close the connection
    conn.close()
    print('All tables updated. Sleeping...')
    return

if __name__ == '__main__':
    print('App started.')
    while True:
        main()
        time.sleep(3660)