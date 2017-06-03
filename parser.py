#!/usr/bin/python3
"""Races parser for Strava website."""
import json
import time
import logging
from datetime import datetime, timedelta

import requests
import pymysql
from config import API_KEY, DB_HOST, DB_USER, DB_NAME, DB_PASSWORD, ATHLETE

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s [%(levelname)s] %(message)s')
headers = {'Authorization': 'Bearer {}'.format(API_KEY)}
st_time = datetime.utcnow()
endpoint = 'running_races'


def get_strava():
    """Parsing JSON races from Strava."""
    def unix_time(date_obj):
        """Create a UNIX time object from datetime one."""
        return time.mktime(date_obj.timetuple())

    def mysql_time(date_str):
        """Create MySQL-compatible datetime object."""
        return datetime.strptime(date_str, '%Y-%m-%dT%H:%M:%SZ')

    conn = pymysql.connect(host=DB_HOST,
                           user=DB_USER,
                           password=DB_PASSWORD,
                           database=DB_NAME)
    while True:
        # Honoring minute/seconds quota
        time.sleep(0.5)
        if len(ATHLETE) > 0:
            endpoint = 'athletes/{}'.format(ATHLETE)
        r = requests.get('https://www.strava.com/api/v3/{}'.
                         format(endpoint), headers=headers)
        limit = list(map(int, r.headers['X-RateLimit-Limit'].split(',')))
        usage = list(map(int, r.headers['X-RateLimit-Usage'].split(',')))
        if usage[1] > limit[1]:
            logging.warning('Daily requests quota depleted!')
            utc_midnight = (st_time + timedelta(days=1)).date()
            sleep_time = unix_time(utc_midnight) - unix_time(st_time)
            logging.info('Sleeping for {} minutes...'.
                         format(round(sleep_time/60, 1)))
            time.sleep(sleep_time)
        elif usage[0] > limit[0]:
            logging.warning('Short-term requests quota depleted!')
            sleep_time = unix_time(st_time +
                                   timedelta(minutes=15)) - unix_time(st_time)
            logging.info('Sleeping for 15 minutes...')
            time.sleep(sleep_time)
        else:
            break
    with conn.cursor() as cur:
        raw_data = json.loads(r.text)
        for i in [raw_data]:
            if len(ATHLETE) > 0:
                i['created_at'] = mysql_time(i['created_at'])
                i['updated_at'] = mysql_time(i['updated_at'])
            else:
                i['start_date_local'] = mysql_time(i['start_date_local'])
            keys = list(i.keys())
            values = list(i.values())
            q = """REPLACE INTO {} {} VALUES({})""".format(endpoint.
                                                           split('/')[0],
                                                           tuple(keys),
                                                           ', '.join(['%s'
                                                                      for i in
                                                                      keys]))
            cur.execute(q.replace("'", ''), values)
        conn.commit()
    conn.close()


if __name__ == "__main__":
    get_strava()
