from pymongo import MongoClient

import pandas as pd
import numpy as np

import pickle
import datetime
import json
import re


def setup_mongo_client(db_name, collection_name, client=None, address='mongodb://localhost:27017/'):
    if not client:
        client = MongoClient(address)
    else:
        client = client
    db = client[db_name]
    collection = db[collection_name]
    return client, collection

def instantiate_metar_dataframe():
    col = ['distant_remark', 'distant_E', 'distant_W', 'distant_S', 'distant_W']
    df = pd.DataFrame(columns=col)
    return df
def add_metar_to_df(metar_string, df):
    pass


def create_full_metar_dataframe(collection, df):
    added_counter = 0
    cursor = collection.find({ "metar_data" : { "$exists" : True },
                                "added_to_dataframe" : { "$exists" : False }},
                                no_cursor_timeout=True)

    for record in cursor:
        identity = record['id']
        metar_string = record['metar_data']
        add_metar_to_df(metar_string, df)
        collection.update_one({"_id": record["_id"]}, {"$set": {"added_to_dataframe": 1}})
        added_counter +=1
    string_report = "marked {} mongo documents".format(added_counter)
    return df

def main(social_media='flickr'):
    filepath = "/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/{}_metar_df".format(social_media)
    client, collection = setup_mongo_client('capstone', '{}_us_rainbows'.format(social_media))
    try:
        df = pd.read_pickle(filepath)
    except:
        df = instantiate_metar_dataframe()
    df = create_full_metar_dataframe(collection, df)
