from pymongo import MongoClient
import pandas as pd
import numpy as np

import pickle
import time
import datetime
import json
import requests
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


def initialize_mongo_collection(df, collection):
    for i in df.index:
        collection.insert_one({'id': df.loc[i, 'id'],
                                'YYYYMMDDHHmm_start': df.loc[i, 'YYYYMMDDHHmm_start'],
                                'YYYYMMDDHHmm_end': df.loc[i, 'YYYYMMDDHHmm_end'],
                                'closest_station': df.loc[i, 'closest_station']})

def get_metar_reports(collection):
    ua = UserAgent()
    proxies = {'https' : get_proxy()}
    added_counter = 0
    skipped_counter = 0
    cursor = collection.find({ "metar_data" : { "$exists" : False } }, no_cursor_timeout=True)
    for record in cursor:
        url = 'http://www.ogimet.com/cgi-bin/getmetar/'
        params = get_metar_params(record)
        try:
            try:
                html = requests.get(url, params=params, proxies=proxies, headers={"User-Agent": ua.random})
            except Exception as e1:
                print("sleeping for 3 seconds because request failed, exception: {}".format(e1))
                html = requests.get(url, params=params ,proxies=proxies, headers={"User-Agent": ua.random})
                time.sleep(3)
        except Exception as e2:
            skipped_counter += 1
            with open('exception_log_ogimet.txt', "a") as myfile:
                myfile.write("exception for url {}: exception1: {}\n exception2: {}".format(url, e1, e2))
            print("exception for url {}: exception1: {}\n exception2: {}".format(url, e1, e2))
            print("had already added {} reports and skipped {} records".format(added_counter, skipped_counter))
            continue
        if html.status_code == 200:
            metar_content = html.content
            collection.update_one({"_id": record["_id"]}, {"$set": {'metar_data': metar_content}})
            added_counter += 1
        else:
            skipped_counter += 1
            with open('status_code_log_ogimet.txt', "a") as myfile:
                myfile.write("status code for url {}: {}\n {} \n{}".format(url, html.status_code, html.content, html.headers))
            print('encountered status code {} for url {}'.format(html.status_code, url))
            print("had already added {} reports and skipped {} records".format(added_counter, skipped_counter))
            continue
        time.sleep(3)
        string_report = "added {} reports and skipped {} records".format(added_counter, skipped_counter)
        print(string_report)



def get_metar_params(record):
    YYYYMMDDHHmm_start = record['YYYYMMDDHHmm_start']
    YYYYMMDDHHmm_end = record['YYYYMMDDHHmm_end']
    icao = record['closest_station']
    ogimet_params = {
            'begin': YYYYMMDDHHmm_start,
            'end': YYYYMMDDHHmm_end,
            'lang': 'eng',
            'header': 'no',
            'icao': icao
            }
    return ogimet_params

def main(client_text='capstone', collection_text='flickr_us_rainbow'):
    client, collection = setup_mongo_client(client_text, collection_text)
    df = pd.read_pickle('flickr_with_station_and_time.p')
    initialize_mongo_collection(df, collection)
    print('initialized collection')
    # get_metar_reports(collection)
