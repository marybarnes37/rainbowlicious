from pymongo import MongoClient
import pandas as pd
import time
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


def get_api_key(machine='local'):
    if machine == 'local':
        path = '/Users/marybarnes/.ssh/weather.txt'
    elif machine == 'ec2':
        path = os.path.join(os.environ['HOME'],'weather.txt')
    with open(path,'r') as f:
        api_key = f.readline().strip()
    return api_key


def add_daily_weather():
    client, collection = setup_mongo_client('capstone', 'seattle_historical_weather_test')
    cursor = collection.find({'bad_solar_angle' : {"$exists" : True}, "daily_weather": {"$exists" : False}}, no_cursor_timeout=True)
    added_counter = 0
    skipped = 0
    proxies = {'http' : get_proxy()}
    path_start = os.path.join(os.environ['HOME'],'start_dates.txt')
    path_end = os.path.join(os.environ['HOME'],'end_dates.txt')
    with open(path_start) as start_file, open(path_end) as end_file:
        for start, end in zip(start_file, end_file):
            start = start.strip()
            end = end.strip()
            url = construct_weather_url(start, end)
            try:
                try:
                    r = requests.get(url, proxies = proxies)
                except Exception as e1:
                    print("sleeping for 5 seconds because request failed, exception: {}".format(str(e1)))
                    with open('weather_errors_and_status_log.txt', "a") as myfile:
                        myfile.write(str(e1))
                    time.sleep(5)
                    r = requests.get(url, proxies = proxies)
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
                monthly_weather = r.json()['observations']
                for observation in monthly_weather:
                    collection.insert_one(observation)
                added_counter += 1
            else:
                print('encountered status code {} for url {}'.format(r.status_code, url))
                with open('weather_errors_and_status_log.txt', "a") as myfile:
                    myfile.write('encountered status code {} for url {}'.format(r.status_code, url))
                skipped += 1
            total = collection.find().count()
            print('added {} weather dicts and skipped {}'.format(added_counter, skipped))
            print('a total of {} local dates have been added'.format(total))
            time.sleep(2)
            if added_counter == 5:
                break


def construct_weather_url(startDate, endDate, lat = '47.33', lon = '-122.19'):
    my_apikey = get_api_key()
    url = "http://api.weather.com/v1/geocode/" + lat + "/" + lon+ \
    "/observations/historical.json?apiKey=" + str(my_apikey) + \
    "&language=en-US" + "&startDate="+ startDate + "&endDate="+ endDate
    url = str.strip(url)
    return url
