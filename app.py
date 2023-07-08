import os
import time
from datetime import datetime, timedelta

import numpy as np
import psycopg2
import pytz
import requests
from dateutil import parser

# set timezone
tz = pytz.timezone('US/Eastern')

def update_activity(conn):
    print('Updating table: activity')

    # get last row in table
    cursor = conn.cursor()
    cursor.execute('SELECT time, android_detected_activity FROM activity ORDER BY time DESC LIMIT 1')
    result = cursor.fetchone()
    last_time = result[0]
    last_state = result[1]

    # get data from api
    response = requests.get(
        os.environ.get('HASS_URL')+'/api/history/period/'+last_time.strftime('%Y-%m-%dT%H:%M:%S%z'),
        headers={
            'authorization': 'Bearer '+os.environ.get('HASS_API_KEY'),
            'content-type': 'application/json',
        },
        params={
            'filter_entity_id': 'sensor.justin_pixel5_detected_activity',
            'end_time': datetime.now().astimezone(tz).strftime('%Y-%m-%dT%H:%M:%S%z'),
            'no_attributes': True
        }
        ).json()
    
    # collate data to submit
    acceptable_states = ['still', 'walking', 'running', 'in_vehicle']
    data_prep = [{'time': parser.isoparse(row['last_changed']), 'state': row['state']} for row in response[0] if row['state'] in acceptable_states]
    data_submit = [{'state':last_state}]
    for row in data_prep:
        if row['state'] != data_submit[-1]['state']:
            data_submit.append(row)
    data_submit.pop(0)
    
    # push data to database
    print('Inserting', len(data_submit), 'rows...')
    for row in data_submit:
        cursor.execute('INSERT INTO activity (time, android_detected_activity) VALUES (%s, %s)',
            (row['time'], row['state'])
            )
    conn.commit()
    cursor.close()

def update_location(conn):
    print('Updating table: location')

    # get last row in table
    cursor = conn.cursor()
    cursor.execute('SELECT time, hass_detected_zone FROM location ORDER BY time DESC LIMIT 1')
    result = cursor.fetchone()
    last_time = result[0]
    last_state = result[1]

    # get data from api
    response = requests.get(
        os.environ.get('HASS_URL')+'/api/history/period/'+last_time.strftime('%Y-%m-%dT%H:%M:%S%z'),
        headers={
            'authorization': 'Bearer '+os.environ.get('HASS_API_KEY'),
            'content-type': 'application/json',
        },
        params={
            'filter_entity_id': 'person.justin',
            'end_time': datetime.now().astimezone(tz).strftime('%Y-%m-%dT%H:%M:%S%z'),
            'no_attributes': True
        }
        ).json()
    
    # collate data to submit
    data_prep = [{'time': parser.isoparse(row['last_changed']), 'state': row['state']} for row in response[0]]
    data_submit = [{'state':last_state}]
    for row in data_prep:
        if row['state'] != data_submit[-1]['state']:
            data_submit.append(row)
    data_submit.pop(0)
    
    # push data to database
    print('Inserting', len(data_submit), 'rows...')
    for row in data_submit:
        cursor.execute('INSERT INTO location (time, hass_detected_zone) VALUES (%s, %s)',
            (row['time'], row['state'])
            )
    conn.commit()
    cursor.close()

def update_device_active(conn):
    print('Updating table: device_active')

    # configuration options
    bucket_width = timedelta(minutes=60)
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
    cursor.execute('SELECT time FROM device_active ORDER BY time DESC LIMIT 1')
    result = cursor.fetchone()
    last_time = result[0]

    # get data from api
    response = requests.get(
        os.environ.get('HASS_URL')+'/api/history/period/'+last_time.strftime('%Y-%m-%dT%H:%M:%S%z'),
        headers={
            'authorization': 'Bearer '+os.environ.get('HASS_API_KEY'),
            'content-type': 'application/json',
        },
        params={
            'filter_entity_id': 
                'binary_sensor.justin_pixel5_device_locked,'+
                'binary_sensor.office_door,'+
                'media_player.fireplace_tv',
            'end_time': datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z'),
            'no_attributes': True
        }
        ).json()
    response_list = [item for sublist in response for item in sublist]

    # build time buckets
    dt = last_time + bucket_width
    buckets = []
    while dt < datetime.now().astimezone(tz):
        buckets.append({
            'end_time': dt,
            'pixel_screen_on': 0,
            'office_door_open': 0,
            'fireplace_tv_on': 0,
            })
        dt += bucket_width

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
            current_timestamp = parser.isoparse(row['last_changed']).astimezone(tz)
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
        cursor.execute('INSERT INTO device_active (time, pixel_screen_on, office_door_open, fireplace_tv_on) VALUES (%s, %s, %s, %s)',
            (row['end_time'], row['pixel_screen_on'], row['office_door_open'], row['fireplace_tv_on'])
            )
    conn.commit()
    cursor.close()

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

    # close the connection
    conn.close()
    print('All tables updated. Sleeping...')

if __name__ == '__main__':
    print('App started.')
    while True:
        main()
        time.sleep(3600)