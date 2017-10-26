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
import pysolar

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


def get_local_time(record, tzwhere_obj):
    local_tz = get_time_zone(record, tzwhere_obj)
    utc_time = record['datetime']
    return local_tz.fromutc(utc_time)


def add_local_dates():
    client, collection = setup_mongo_client('capstone', 'insta_rainbow')
    cursor = collection.find({"start_date_local" : { "$exists" : False }}, no_cursor_timeout=True)
    tzwhere_obj = tzwhere.tzwhere()
    added_counter = 0
    deleted_counter = 0
    for record in cursor:
        local_datetime = get_local_time(record, tzwhere_obj)
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



def find_closest_observation():
    client, collection = setup_mongo_client('capstone', 'insta_rainbow')
    cursor = collection.find({"daily_weather": {"$exists" : True}, "closest_observation" : {"$exists": False}}, no_cursor_timeout=True)
    added_counter = 0
    skipped = 0
    for record in cursor:
        rainbow_time = ar.timegm(record['datetime'].timetuple())
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


def check_thirty_miles():
    client, collection = setup_mongo_client('capstone', 'insta_rainbow')
    cursor = collection.find({"closest_observation" : {"$exists": True}, "within_thirty_miles": {"$exists" : False}}, no_cursor_timeout=True)
    with open("/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/metar_dict.p",'r') as f:
        metar_dict = pickle.load(f)
    close_counter = 0
    far_counter = 0
    skipped_counter = 0
    for record in cursor:
        try:
            miles = vincenty((record['latitude'], record['longitude']), metar_dict[record['closest_observation']['obs_id']]).miles
        except:
            with open("/Users/marybarnes/capstone_galvanize/stations_not_in_metar_dict", 'wb') as f:
                f.write(record['closest_observation']['obs_id'])
            print(record['closest_observation']['obs_id'])
            skipped_counter += 1
            continue
        if miles < 35:
            close_counter += 1
            collection.update_one({"_id": record["_id"]}, {"$set": {"within_thirty_miles": 1 }})
        else:
            far_counter += 1
            collection.update_one({"_id": record["_id"]}, {"$set": {"within_thirty_miles": 0 }})
        print('close {}, far {}, skipped because station not METAR {}'.format(close_counter, far_counter, skipped_counter))
    client.close()

def create_dataframe_to_check_duplicates(collection):
    cursor = collection.find({"within_thirty_miles": 1}, no_cursor_timeout=True)
    col = ['_id', 'datetime', 'obs_id']
    df = pd.DataFrame(columns=col)
    for record in cursor:
        df = df.append(pd.DataFrame(
                        [[record['_id'], record['datetime'],
                          record['closest_observation']['obs_id']]],  columns=col), ignore_index=True)
    cursor.close()
    pd.to_pickle(df, "/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/group_by_insta_df.p")
    return df


def create_duplicates_list(duplicates_filename = "/Users/marybarnes/capstone_galvanize/insta_duplicates.txt",
                    pickled_df = "/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/group_by_insta_df.p"):
    client, collection = setup_mongo_client('capstone', 'insta_rainbow')
    if pickled_df:
        df = pd.read_pickle(pickled_df)
    else:
        df = create_dataframe_to_check_duplicates(collection)
    df = df.sort_values([ 'obs_id', 'datetime'])
    f = open(duplicates_filename, 'w')
    previous_station = "x"
    previous_epoch = 0
    for i in df.index:
        station = df.loc[i, 'obs_id']
        obs_epoch = calendar.timegm(df.loc[i, 'datetime'].timetuple())
        if previous_station == station:
            if abs(obs_epoch - previous_epoch) < 1800:
                f.write(str(df.loc[i, '_id']) + '\n')
        previous_station = station
        previous_epoch = obs_epoch
    f.close()
    client.close()


def mark_duplicates(duplicates_filename = "/Users/marybarnes/capstone_galvanize/insta_duplicates.txt"):
    client, collection = setup_mongo_client('capstone', 'insta_rainbow')
    with open(duplicates_filename, 'r') as f:
        for _id in f:
            collection.update_one({"_id": ObjectId(_id.strip())}, {"$set": {"duplicate": 1}}, upsert=False)
    client.close()


def mark_non_duplicates():
    client, collection = setup_mongo_client('capstone', 'insta_rainbow')
    collection.update_many({"within_thirty_miles": 1, "duplicate": {"$exists" : False}}, {"$set": {"duplicate": 0}})
    client.close()



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
    return local_tz


# def add_timezone():
#     client, collection = setup_mongo_client('capstone', 'insta_rainbow')
#     cursor = collection.find({"duplicate": 0, "timezone": {"$exists" : False}},  no_cursor_timeout=True)
#     tzwhere_obj = tzwhere.tzwhere()
#     added_counter = 0
#     for record in cursor:
#         time_zone = get_time_zone(record, tzwhere_obj)
#         collection.update_one({"_id": record["_id"]}, {"$set": {'timezone': time_zone }})
#         added_counter += 1
#         total = collection.find({'timezone' : { "$exists" : True }}).count()
#         print('added {} timezone for total of {}'.format(added_counter, total))
#     client.close()


def main(client_text='capstone', collection_text='insta_rainbow'):
    client, collection = setup_mongo_client(client_text, collection_text)
    # add_local_dates(collection)
    # add_daily_weather(collection)
