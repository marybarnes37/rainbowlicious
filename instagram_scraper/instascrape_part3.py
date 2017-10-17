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

def add_lat_long(collection):
    cursor = collection.find({ "latitude" : { "$exists" : False }, "location_name" : { "$exists" : True } }, no_cursor_timeout=True)
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
    cursor.close()


def filter_US_locations(collection):
    cursor = collection.find({ "location_dict" : { "$exists" : False }, "latitude" : { "$exists" : True }}, no_cursor_timeout=True)
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
    cursor.close()

def main():
    client, collection = setup_mongo_client('capstone', 'insta_rainbow')
    add_lat_long(collection)
    print('added longitude and latitude')
    filter_US_locations(collection)
    print('filtered for US locations')
