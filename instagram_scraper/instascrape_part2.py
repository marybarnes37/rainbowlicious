from pymongo import MongoClient
from geopy.geocoders import Nominatim
import reverse_geocoder

import numpy as np
import time
import datetime
import json
import requests
import re
import csv

def setup_mongo_client(db_name, collection_name, client=None, address='mongodb://localhost:27017/'):
    if not client:
        client = MongoClient(address)
    else:
        client = client
    db = client[db_name]
    collection = db[collection_name]
    return client, collection


def add_urls_and_datetimes(collection):
    cursor = collection.find({ "url" : { "$exists" : False } })
    added_counter = 0
    base_url = 'https://www.instagram.com/p/'
    for record in cursor:
        shortcode = record['node']['shortcode']
        full_url = base_url + shortcode + '/'
        collection.update_one({"_id": record["_id"]}, {"$set": {'url': full_url}})
        timestamp = record['node']['taken_at_timestamp']
        date_time = datetime.datetime.fromtimestamp(timestamp)
        collection.update_one({"_id": record["_id"]}, {"$set": {'datetime': date_time}})
        added_counter += 1
    string_report = "added {} urls and dates ".format(added_counter)
    print(string_report)
    with open('status_reports.txt', "a") as myfile:
        myfile.write(string_report)

def visit_urls_get_locations(collection):
    cursor = collection.find({ "location_name" : { "$exists" : False } })
    added_counter = 0
    deleted_counter = 0
    for record in cursor:
        time.sleep(np.random.randint(8, 12))
        url = record['url']
        html = requests.get(url)
        if html.status_code == 200:
            match = re.search('window._sharedData = (.*);</script>', html.text)
            json_dict = json.loads(match.group(1))
            try:
                location_info = json_dict['entry_data']['PostPage'][0]['graphql']['shortcode_media']['location']
                location_id = location_info['id']
                location_name = location_info['name']
                collection.update_one({"_id": record["_id"]}, {"$set": {'location_id': location_id}})
                collection.update_one({"_id": record["_id"]}, {"$set": {'location_name': location_name}})
                added_counter += 1
            except:
                collection.delete_one({"_id": record["_id"]})
                deleted_counter += 1
        else:
            with open('status_code_log.txt', "a") as myfile:
                myfile.write("status code: {}\n {} \n{}".format(r.status_code, r.content, r.headers))
            print('encountered status code {}'.format(r.status_code))
            print("had already added {} raw locations and deleted {} records".format(added_counter, deleted_counter))
            return None
        string_report = "added {} raw locations and deleted {} records".format(added_counter, deleted_counter)
        print(string_report)
        with open('status_reports.txt', "a") as myfile:
            myfile.write(string_report)


def add_lat_long(collection):
    cursor = collection.find({ "latitude" : { "$exists" : False } })
    geolocator = Nominatim()
    added_counter = 0
    deleted_counter = 0
    for record in cursor:
        geolocation = geolocator.geocode(record['location_name'],timeout=10000)
        if geolocation!=None:
            collection.update_one({"_id": record["_id"]}, {"$set": {'latitude': geolocation.latitude, 'longitude': geolocation.longitude}})
        else:
            collection.delete_one({"_id": record["_id"]})
    string_report = "added {} latitude and longitude and deleted {} records".format(added_counter, deleted_counter)
    print(string_report)
    with open('status_reports.txt', "a") as myfile:
        myfile.write(string_report)


def filter_US_locations(collection):
    cursor = collection.find({ "location_dict" : { "$exists" : False } })
    added_counter = 0
    deleted_counter = 0
    for record in cursor:
        location_data = reverse_geocoder.search((record['latitude'], record['longitude']))
        if location_data[0]['cc'] == 'US':
            collection.update_one({"_id": record["_id"]}, {"$set": {'location_dict': location_data[0]}})
        else:
            collection.delete_one({"_id": record["_id"]})
    string_report = "added {} location dicts and deleted {} records".format(added_counter, deleted_counter)
    print(string_report)
    with open('status_reports.txt', "a") as myfile:
        myfile.write(string_report)

def main():
    client, collection = setup_mongo_client('capstone', 'insta_rainbow')
    add_urls_and_datetimes(collection)
    print('added urls_and_datetimes')
    visit_urls_get_locations(collection)
    print('visited urls and got locations')
    add_lat_long(collection)
    print('added longitude and latitude')
    filter_US_locations(collection)
    print('filtered for US locations')
