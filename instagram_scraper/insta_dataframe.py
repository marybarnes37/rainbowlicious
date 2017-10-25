from pymongo import MongoClient
import pandas as pd
from geopy.distance import vincenty
import reverse_geocoder
import pickle
import time
import datetime
import calendar
import pytz
from tzwhere import tzwhere
import os
import sys

def setup_mongo_client(db_name, collection_name, client=None, address='mongodb://localhost:27017/'):
    if not client:
        client = MongoClient(address)
    else:
        client = client
    db = client[db_name]
    collection = db[collection_name]
    return client, collection
