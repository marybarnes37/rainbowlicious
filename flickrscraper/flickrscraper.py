import os
import sys
import datetime
from dateutil.parser import parse as parsedate

import json

import flickr_api
import pymongo

#use database "capstone" and collection "flickr_rainbow"

def setup_mongo_client(db_name, collection_name, address='mongodb://localhost:27017/'):
    """ Return Mongo client and collection for record insertion.

    Args:
        db_name (str): Database name.
        collection_name (str): Collection name.
        address (Optional[str]): Address to mongo database.
            Defaults to 'mongodb://localhost:27017/)'.

    Returns:
        client (pymongo.MongoClient): Intantiated pymongo client.
        collection (pymongo.Collection): Collection object for record insertion.
    """
    client = MongoClient(address)
    db = client[db_name]
    collection = db[collection_name]
    return client, collection

def get_api_key():
    with open('/Users/marybarnes/.ssh/flickr.txt', 'rb') as f:
        api_key = f.readline()
        secret = f.readline()
    return api_key, secret

def main():
    api_key, secret = get_api_key()
    flickr_api.set_keys(api_key = api_key, api_secret = secret)

if __name__ == '__main__':
    main()
