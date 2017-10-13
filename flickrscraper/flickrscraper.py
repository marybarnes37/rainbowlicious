import os
import sys
import datetime
from dateutil.parser import parse as parsedate

import json

# import flickrapi
import pymongo


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
    print(api_key)
    return api_key
