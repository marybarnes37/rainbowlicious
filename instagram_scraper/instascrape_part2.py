from pymongo import MongoClient
from geopy.geocoders import Nominatim
import reverse_geocoder

import numpy as np
import time
import datetime
import json
import requests
import re
import os
from fake_useragent import UserAgent



def setup_mongo_client(db_name, collection_name, client=None, address='mongodb://localhost:27017/'):
    if not client:
        client = MongoClient(address)
    else:
        client = client
    db = client[db_name]
    collection = db[collection_name]
    return client, collection

def get_proxy():
    path = os.path.join(os.environ['HOME'],'proxy.txt')
    with open(path,'rb') as f:
        proxy = f.readline().strip()
    return proxy

def add_urls_and_datetimes(collection):
    cursor = collection.find({ "url" : { "$exists" : False } }, no_cursor_timeout=True)
    added_counter = 0
    base_url = 'https://www.instagram.com/pexit'
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
    cursor.close()

def visit_urls_get_locations(collection):
    ua = UserAgent()
    proxies = {'https' : get_proxy()}
    cursor = collection.find({ "location_name" : { "$exists" : False } }, no_cursor_timeout=True)
    added_counter = 0
    deleted_counter = 0
    for record in cursor:
        url = record['url']
        try:
            try:
                html = requests.get(url, proxies=proxies, headers={"User-Agent": ua.random})
            except Exception as e1:
                print("sleeping for 2 seconds because request failed, exception: {}".format(e1))
                html = requests.get(url, proxies=proxies, headers={"User-Agent": ua.random})
                time.sleep(2)
        except Exception as e2:
            print(e2)
            collection.delete_one({"_id": record["_id"]})
            deleted_counter += 1
            continue
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
        elif html.status_code == 404:
            collection.delete_one({"_id": record["_id"]})
            deleted_counter += 1
        else:
            with open('status_code_log.txt', "a") as myfile:
                myfile.write("status code for url {}: {}\n {} \n{}".format(url, html.status_code, html.content, html.headers))
            print('encountered status code {} for url {}'.format(html.status_code, url))
            print("had already added {} raw locations and deleted {} records".format(added_counter, deleted_counter))
        time.sleep(np.random.uniform(.1,.4))
        string_report = "added {} raw locations and deleted {} records".format(added_counter, deleted_counter)
        print(string_report)
        with open('status_reports.txt', "a") as myfile:
            myfile.write(string_report)
    cursor.close()


def add_lat_long(collection):
    cursor = collection.find({ "latitude" : { "$exists" : False }, "location_name" : { "$exists" : True } }, no_cursor_timeout=True)
    geolocator = Nominatim()
    added_counter = 0
    deleted_counter = 0
    for record in cursor:
        geolocation = geolocator.geocode(record['location_name'],timeout=10000)
        if geolocation!=None:
            collection.update_one({"_id": record["_id"]}, {"$set": {'latitude': geolocation.latitude, 'longitude': geolocation.longitude}})
            added_counter += 1
        else:
            collection.delete_one({"_id": record["_id"]})
            deleted_counter += 1
        string_report = "added {} latitude and longitude and deleted {} records".format(added_counter, deleted_counter)
        print(string_report)
        time.sleep(1.5)
    cursor.close()


def filter_US_locations(collection):
    cursor = collection.find({ "location_dict" : { "$exists" : False }, "latitude" : { "$exists" : True }}, no_cursor_timeout=True)
    added_counter = 0
    deleted_counter = 0
    for record in cursor:
        location_data = reverse_geocoder.search((record['latitude'], record['longitude']))
        if location_data[0]['cc'] == 'US':
            collection.update_one({"_id": record["_id"]}, {"$set": {'location_dict': location_data[0]}})
            added_counter += 1
        else:
            collection.delete_one({"_id": record["_id"]})
            deleted_counter += 1
        string_report = "added {} location dicts and deleted {} records".format(added_counter, deleted_counter)
        print(string_report)
        time.sleep(1.5)
    cursor.close()

def main():
    client, collection = setup_mongo_client('capstone', 'insta_rainbow')
    add_urls_and_datetimes(collection)
    print('added urls_and_datetimes')
    while True:
        try:
            visit_urls_get_locations(collection)
        except:
            continue
        break
    print('visited urls and got locations')
    # add_lat_long(collection)
    # print('added longitude and latitude')
    # filter_US_locations(collection)
    # print('filtered for US locations')
