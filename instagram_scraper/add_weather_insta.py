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

def setup_mongo_client(db_name, collection_name, client=None, address='mongodb://localhost:27017/'):
    if not client:
        client = MongoClient(address)
    else:
        client = client
    db = client[db_name]
    collection = db[collection_name]
    return client, collection

def get_api_key():
    path = os.path.join(os.environ['HOME'],'weather.txt')
    with open(path,'rb') as f:
        api_key = f.readline().strip()
    return api_key


def get_proxy():
    path = os.path.join(os.environ['HOME'],'proxy.txt')
    with open(path,'rb') as f:
        proxy = f.readline().strip()
    return proxy


def get_time_zone(record, tzwhere_obj):
    lat = record['latitude']
    lon = record['longitude']
    timezone_str = tzwhere_obj.tzNameAt(float(lat), float(lon))
    if timezone_str == None:
        timezone_str = tzwhere_obj.tzNameAt(float(lat)-3, float(lon)-3)
        if timezone_str == None:
            timezone_str = tzwhere_obj.tzNameAt(float(lat)+3, float(lon)+3)
            if timezone_str == None:
                timezone_str = tzwhere_obj.tzNameAt(float(lat)-3, float(lon)+3)
                if timezone_str == None:
                    timezone_str = tzwhere_obj.tzNameAt(float(lat)+3, float(lon)-3)
                    if timezone_str == None:
                        return None
    local_tz = pytz.timezone(timezone_str)
    utc_time = record['datetime']
    return local_tz.fromutc(utc_time)


def add_local_dates():
    client, collection = setup_mongo_client('capstone', 'insta_rainbow')
    cursor = collection.find({"start_date_local" : { "$exists" : False }}, no_cursor_timeout=True)
    tzwhere_obj = tzwhere.tzwhere()
    added_counter = 0
    deleted_counter = 0
    for record in cursor:
        local_datetime = get_time_zone(record, tzwhere_obj)
        if local_datetime == None:
            collection.delete_one({"_id": record["_id"]})
            deleted_counter += 1
        else:
            string_local_datetime = local_datetime.strftime('%Y%m%d %Z')
            collection.update_one({"_id": record["_id"]}, {"$set": {'start_date_local': string_local_datetime }})
            added_counter += 1
            total = collection.find({"start_date_local" : { "$exists" : True }}).count()
        print('added {} local dates and deleted {} records'.format(added_counter, deleted_counter))
        print('a total of {} local dates have been added'.format(total))


def add_daily_weather():
    client, collection = setup_mongo_client('capstone', 'insta_rainbow')
    cursor = collection.find({"start_date_local" : { "$exists" : True }, "daily_weather": {"$exists" : False}}, no_cursor_timeout=True)
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
            if(r.json()['errors']):
                print("[ERROR RETURNED FROM API REQUEST]: {}".format(r.json()['errors'][0]['error']['message']))
                with open('weather_errors_and_status_log.txt', "a") as myfile:
                    myfile.write("[ERROR RETURNED FROM API REQUEST]: {}".format(r.json()['errors'][0]['error']['message']))
                skipped += 1
            else:
                daily_weather = r.json()['observations']
                collection.update_one({"_id": record["_id"]}, {"$set": {'daily_weather': daily_weather }})
                added_counter += 1
        else:
            print('encountered status code {} for url {}'.format(r.status_code, url))
            with open('weather_errors_and_status_log.txt', "a") as myfile:
                myfile.write('encountered status code {} for url {}'.format(r.status_code, url))
            skipped += 1
        total = collection.find({"daily_weather" : { "$exists" : True }}).count()
        print('added {} weather dicts and skipped {}'.format(added_counter, skipped))
        print('a total of {} local dates have been added'.format(total))
        time.sleep(3)

def construct_weather_url(record):
    my_apikey = get_api_key()
    lat = record['latitude']
    lon = record['longitude']
    startDate = record['start_date_local']
    url = "http://api.weather.com/v1/geocode/" + str(lat) + "/" + str(lon)+ \
    "/observations/historical.json?apiKey=" + my_apikey + \
    "&language=en-US" + "&startDate="+str(startDate)[:-4]
    url = str.strip(url)
    return url

def main(client_text='capstone', collection_text='insta_rainbow'):
    client, collection = setup_mongo_client(client_text, collection_text)
    add_local_dates(collection)
    # add_daily_weather(collection)
