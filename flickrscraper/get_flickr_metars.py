from pymongo import MongoClient
import pandas as pd
import reverse_geocoder
import pickle
import time

def setup_mongo_client(db_name, collection_name, client=None, address='mongodb://localhost:27017/'):
    if not client:
        client = MongoClient(address)
    else:
        client = client
    db = client[db_name]
    collection = db[collection_name]
    return client, collection

def initialize_mongo_collection(df, collection):
    for ident in df['id']:
        collection.insert_one({'id': ident})

def

def main(client_text='capstone', collection_text='flickr_us_rainbows'):
    client, collection = setup_mongo_client(client_text, collection_text)
    df = pd.read_pickle('/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/flickr_with_stations.p')
    initialize_mongo_collection(df, collection)
