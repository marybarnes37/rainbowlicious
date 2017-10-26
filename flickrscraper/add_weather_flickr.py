from pymongo import MongoClient
import pandas as pd
import reverse_geocoder
import pickle
import time
import pytz
from tzwhere import tzwhere
from geopy.distance import vincenty
import datetime
import os
import sys
import requests
import calendar
from bson.objectid import ObjectId
from pysolar.solar import get_altitude


def setup_mongo_client(db_name, collection_name, client=None, address='mongodb://localhost:27017/'):
    if not client:
        client = MongoClient(address)
    else:
        client = client
    db = client[db_name]
    collection = db[collection_name]
    return client, collection


def get_api_key(machine='local'):
    if machine == 'local':
        path = '/Users/marybarnes/.ssh/weather.txt'
    elif machine == 'ec2':
        path = os.path.join(os.environ['HOME'],'weather.txt')
    with open(path,'r') as f:
        api_key = f.readline().strip()
    return api_key


def get_string_date(record):
    recorded_time = record['raw_json']['datetaken']
    api_date = recorded_time[:4] + recorded_time[5:7] + recorded_time[8:10]
    return api_date


def add_dates():
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle_w_dates')
    cursor = collection.find({'bad_solar_angle' : {"$exists" : True}, "start_date_local" : { "$exists" : False }}, no_cursor_timeout=True)
    added_counter = 0
    for record in cursor:
        date = get_string_date(record)
        collection.update_one({"_id": record["_id"]}, {"$set": {'start_date_local': date }})
        added_counter += 1
        total = collection.find({"start_date_local" : { "$exists" : True }}).count()
        print('added {} local dates'.format(added_counter))
        print('a total of {} local dates have been added'.format(total))


def add_daily_weather():
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle_w_dates')
    cursor = collection.find({'bad_solar_angle' : {"$exists" : True}, "daily_weather": {"$exists" : False}}, no_cursor_timeout=True)
    added_counter = 0
    skipped = 0
    proxies = {'http' : get_proxy()}
    for record in cursor:
        url = construct_weather_url(record)
        try:
            try:
                r = requests.get(url, proxies=proxies)
            except Exception as e1:
                print("sleeping for 5 seconds because request failed, exception: {}".format(str(e1)))
                with open('weather_errors_and_status_log.txt', "a") as myfile:
                    myfile.write(str(e1))
                time.sleep(5)
                r = requests.get(url, proxies=proxies)
        except Exception as e2:
            print("SKIPPING because request failed, exception: {}".format(str(e2)))
            with open('weather_errors_and_status_log.txt', "a") as myfile:
                myfile.write(str(e2))
            time.sleep(5)
            skipped += 1
            continue
        if r.status_code == 200:
            try:
                if(r.json()['errors']):
                    print("[ERROR RETURNED FROM API REQUEST]: {}".format(r.json()['errors'][0]['error']['message']))
                    with open('weather_errors_and_status_log.txt', "a") as myfile:
                        myfile.write("[ERROR RETURNED FROM API REQUEST]: {}".format(r.json()['errors'][0]['error']['message']))
                    skipped += 1
            except:
                pass
            daily_weather = r.json()['observations']
            collection.update_one({"_id": record["_id"]}, {"$set": {'daily_weather': daily_weather }})
            added_counter += 1
        else:
            print('encountered status code {} for url {}'.format(r.status_code, url))
            with open('weather_errors_and_status_log.txt', "a") as myfile:
                myfile.write('encountered status code {} for url {}'.format(r.status_code, url))
            skipped += 1
        total = collection.find({'bad_solar_angle' : {"$exists" : True}, "daily_weather" : { "$exists" : True }}).count()
        print('added {} weather dicts and skipped {}'.format(added_counter, skipped))
        print('a total of {} local dates have been added'.format(total))
        time.sleep(2)


def construct_weather_url(record, lat = '47.33', lon = '-122.19'):
    my_apikey = get_api_key()
    startDate = record['start_date_local']
    url = "http://api.weather.com/v1/geocode/" + lat + "/" + lon+ \
    "/observations/historical.json?apiKey=" + str(my_apikey) + \
    "&language=en-US" + "&startDate="+startDate
    url = str.strip(url)
    return url


def get_epoch_time(record, tz):
    local_time = record['raw_json']['datetaken']
    pattern = '%Y-%m-%d %H:%M:%S'
    datetime_object = datetime.datetime.strptime(local_time, pattern)
    tz_aware_dt = tz.localize(datetime_object)
    obs_epoch = time.mktime(tz_aware_dt.timetuple())
    return obs_epoch


def lookup_timezone(station):
    with open('/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/metar_timezone_dict.p', 'rb') as f:
        metar_timezone_dict = pickle.load(f, encoding='latin1')
    return metar_timezone_dict[station][1]


def get_proxy(machine='local'):
    if machine == 'local':
        path = '/Users/marybarnes/.ssh/proxy.txt'
        with open(path,'r', encoding='latin-1') as f:
            proxy = f.readline().strip()
        # proxy = 'http://' + str(proxy)
    elif machine == 'ec2':
        path = os.path.join(os.environ['HOME'],'proxy.txt')
        with open(path,'r', encoding='latin-1') as f:
            proxy = f.readline().strip()
    return str(proxy)


def find_closest_observation(station='KBFI'):
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle_w_dates')
    cursor = collection.find({'bad_solar_angle' : {"$exists" : True}, "daily_weather": {"$exists" : True}, "closest_observation" : {"$exists": False}}, no_cursor_timeout=True)
    added_counter = 0
    skipped = 0
    tz = lookup_timezone(station)
    for record in cursor:
        rainbow_time = get_epoch_time(record, tz)
        shortest_time_between = 4000
        index_of_closest_obs = -1
        index_of_obs_before = -1
        for i, obs in enumerate(record['daily_weather']):
            time_between = abs(rainbow_time - obs['valid_time_gmt'])
            if time_between < shortest_time_between:
                shortest_time_between = time_between
                index_of_closest_obs = i
                index_of_obs_before = i-1
        if shortest_time_between < 4000:
            collection.update_one({"_id": record["_id"]}, {"$set": {"closest_observation": record['daily_weather'][index_of_closest_obs] }})
            collection.update_one({"_id": record["_id"]}, {"$set": {"observation_before_closest": record['daily_weather'][index_of_obs_before] }})
            added_counter += 1
        else:
            skipped += 1
            print("SKIPPED ONE: shortest time: {}".format(shortest_time_between))
        print('added {}, skipped {}'.format(added_counter, skipped))
    client.close()


def lookup_timezone(station):
    with open('/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/metar_timezone_dict.p', 'rb') as f:
        metar_timezone_dict = pickle.load(f, encoding='latin1')
    return metar_timezone_dict[station][1]


def lookup_lat_lon(station):
    with open("/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/metar_dict.p",'rb') as f:
        metar_dict = pickle.load(f, encoding='latin1')
    return metar_dict[station][0], metar_dict[station][1]


def add_solar_angle_of_observations(station="KSEA"):
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle_w_dates', address='mongodb://localhost:27017/')
    local_tz = lookup_timezone(station)
    latitude, longitude = lookup_lat_lon(station)
    cursor = collection.find( {"closest_observation" : {"$exists": True}, "closest_observation.solar_angle" : {"$exists": False}}, no_cursor_timeout=True)
    added_counter = 0
    for record in cursor:
        epoch_time = record['closest_observation']['valid_time_gmt']
        prev_epoch_time = record['observation_before_closest']['valid_time_gmt']
        dt_obj = datetime.datetime.fromtimestamp(epoch_time)
        prev_dt_obj = datetime.datetime.fromtimestamp(prev_epoch_time)
        solar_angle = get_altitude(latitude, longitude, dt_obj)
        prev_solar_angle = get_altitude(latitude, longitude, prev_dt_obj)
        # print("recorded time of rainbow: {}".format(record['raw_json']['datetaken']))
        # print("observation in epoch time: {}".format(epoch_time))
        # print("observation_local_time_stamp {}:".format(dt_obj))
        # print(prev_solar_angle)
        # print(solar_angle)
        # print("\n")
        collection.update_one({"_id": record["_id"]}, {"$set": {"closest_observation.solar_angle": solar_angle}})
        collection.update_one({"_id": record["_id"]}, {"$set": {"observation_before_closest.solar_angle": prev_solar_angle}})
        added_counter += 1
        print("added {}".format(added_counter))
    client.close()

def main():
    add_dates()
    add_daily_weather()
    find_closest_observation()
