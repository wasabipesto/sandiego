import os
import time
from datetime import datetime, timedelta

import numpy as np
import psycopg2
import requests
from dateutil import parser

def update_activity(conn):
    print('Updating table: activity')

    # get last row in table
    cursor = conn.cursor()
    cursor.execute("SELECT time, android_detected_activity FROM activity ORDER BY time DESC LIMIT 1")
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
            'end_time': datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z'),
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
        cursor.execute("INSERT INTO activity (time, android_detected_activity) VALUES (%s, %s)",
            (row["time"], row["state"])
            )
    conn.commit()
    cursor.close()

def update_location(conn):
    print('Updating table: location')

    # get last row in table
    cursor = conn.cursor()
    cursor.execute("SELECT time, hass_detected_zone FROM location ORDER BY time DESC LIMIT 1")
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
            'end_time': datetime.now().strftime('%Y-%m-%dT%H:%M:%S%z'),
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
        cursor.execute("INSERT INTO location (time, hass_detected_zone) VALUES (%s, %s)",
            (row["time"], row["state"])
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

    # close the connection
    conn.close()
    print('All tables updated. Sleeping...')

if __name__ == "__main__":
    print('App started.')
    while True:
        main()
        time.sleep(3600)