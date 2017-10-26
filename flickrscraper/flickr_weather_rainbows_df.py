from pymongo import MongoClient
import pandas as pd
import numpy as np
import pickle
from pysolar.solar import get_altitude
# import time
# import datetime
# import calendar
# import pytz
# from tzwhere import tzwhere
# import os
# from bson.objectid import ObjectId


def setup_mongo_client(db_name, collection_name, client=None, address='mongodb://localhost:27017/'):
    if not client:
        client = MongoClient(address)
    else:
        client = client
    db = client[db_name]
    collection = db[collection_name]
    return client, collection


def create_flickr_weather_rainbow_df(pickle_filename = "/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/flickr_weather_rainbows_df.p"):
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle_w_dates')
    col = get_columns(collection)
    df = pd.DataFrame(columns=col)
    df = get_rainbows_from_mongo(df, collection)
    df = add_other_weather_data(df, collection)
    pd.to_pickle(df, pickle_filename)
    client.close()
    return df


def get_rainbows_from_mongo(df, collection):
    cursor = collection.find({"label": '1', "bad_solar_angle": 0, "closest_observation" : {"$exists": True}}, no_cursor_timeout=True)
    for record in cursor:
        data, columns = get_line(record, rainbow=1)
        df = df.append(pd.DataFrame(data, columns=columns), ignore_index=True)
    return df

def add_other_weather_data(df, collection):
    cursor = collection.find({"label": '0', "closest_observation" : {"$exists": True}}, no_cursor_timeout=True)
    for record in cursor:
        data, columns = get_line(record, rainbow=0)
        df = df.append(pd.DataFrame(data, columns=columns), ignore_index=True)
    return df

def get_columns(collection):
    example = collection.find_one({"label": '1', "bad_solar_angle": 0, "closest_observation" : {"$exists": True}})
    columns = list(example['closest_observation'].keys())
    prev_columns = []
    for entry in columns:
        prev_columns.append("prev_" + entry)
    columns.extend(prev_columns)
    columns.extend(['_id', 'rainbow'])
    return columns

def get_line(record, rainbow):
    closest_items = record['closest_observation'].items()
    previous_items = record['observation_before_closest'].items()
    data_list = []
    columns = []
    for column, data in closest_items:
        columns.append(column)
        data_list.append(data)
    for column, data in previous_items:
        columns.append("prev_" + column)
        data_list.append(data)
    data_list.extend([record['_id'], rainbow])
    columns.extend(['_id', 'rainbow'])
    return [data_list], columns

def drop_none_columns(df, pickle_filename ='/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/none_columns.p'):
    if pickle_filename:
        columns = pickle.load(open(pickle_filename, 'rb'))
    else:
        none_dict = {}
        for column in df.columns:
            if len(df[column].unique()) == 1:
                none_dict[column] = df[column].unique()
        columns = list(none_dict.keys())
    df.drop(columns, axis=1, inplace=True)
    pd.to_pickle(df, '/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/flickr_rainbows_dropped_nones.p')
    return df


def pickle_df(df, filename):
    filename = "/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/" + filename + ".p"
    pd.to_pickle(df, filename)


def drop_times_icons_names(df):
    # do I want to drop key?
    columns_to_drop = ['expire_time_gmt', u'wx_icon', u'obs_id', u'valid_time_gmt', u'obs_name',
                      u'key', 'prev_expire_time_gmt', u'prev_wx_icon', u'prev_obs_id', u'prev_valid_time_gmt',
                        u'prev_obs_name', u'prev_key', 'day_ind', 'icon_extd', 'prev_day_ind', 'prev_icon_extd']
    df.to_pickle('/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/dropped_time_icon_names.p')
    return df



def drop_identifiers(df):
    columns_to_drop = ['_id']
    return df


def fill_missing_values(df):
    return df


def add_dummies(df):
    df = pd.get_dummies(df, columns = ['clds', 'pressure_desc',
              'uv_desc', 'wdir_cardinal', 'wx_phrase',
              'prev_clds', 'prev_pressure_desc',
              'prev_uv_desc', 'prev_wdir_cardinal', 'prev_wx_phrase'] )
    df.to_pickle('/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/flickr_rainbows_with_dummies.p')
    return df


# def drop_non_tz_rows(df):
#     non_tz_stations = ['PACZ', 'PASN', 'PADK', 'PMDY', 'PASY', 'PHLU', 'KQT9',
#                       'KEGC', 'KH78', 'KATP', 'KGBK', 'PHLI', 'KGRY', 'PHHI',
#                       'KXCN', 'PACD', 'PHHN', 'K25T', 'PAOU', 'PAUT', 'KHHV',
#                       'PHJR', 'PAKF', 'K28K','KIKT','PHOG','PAPB','PHNY','PHKO',
#                       'PHJH','PADU','KGHB','PALU']
#     df = df[~df['obs_id'].isin(non_tz_stations)]
#     return df
#
