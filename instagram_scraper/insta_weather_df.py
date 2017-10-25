from pymongo import MongoClient
import pandas as pd
import numpy as np
from geopy.distance import vincenty
import reverse_geocoder
import pickle
import time
import datetime
import calendar
import pytz
from tzwhere import tzwhere
import os
from bson.objectid import ObjectId
from pysolar.solar import get_altitude


def create_insta_weather_df(pickle_filename = "/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/insta_weather_df.p"):
    client, collection = setup_mongo_client('capstone', 'insta_rainbow')
    cursor = collection.find({"duplicate": 0}, no_cursor_timeout=True)
    col = get_columns(collection)
    df = pd.DataFrame(columns=col)
    for record in cursor:
        data, columns = get_line(record)
        df = df.append(pd.DataFrame(data, columns=columns), ignore_index=True)
    pd.to_pickle(df, pickle_filename)
    client.close()
    return df

def get_columns(collection):
    example = collection.find_one({'duplicate': 0})
    columns = list(example['closest_observation'].keys())
    prev_columns = []
    for entry in columns:
        prev_columns.append("prev_" + entry)
    columns.extend(prev_columns)
    columns.extend(['_id', 'url'])
    return columns

def get_line(record):
    closest_items = record['closest_observation'].items()
    previous_items = record['observation_before_closest'].items()
    data_list = []
    columns = []
    for column, data in closest_items:
        columns.append(column)
        data_list.append(data, )
    for column, data in previous_items:
        columns.append("prev_" + column)
        data_list.append(data)
    data_list.extend([record['_id'], record['url']])
    columns.extend(['_id', 'url'])
    return [data_list], columns

def drop_none_columns(df, pickle_filename =
                      '/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/none_columns.p'):
    if pickle_filename:
        columns = pickle.load(open(pickle_filename, 'rb'))
    else:
        none_dict = {}
        for column in df.columns:
            if len(df[column].unique()) == 1:
                none_dict[column] = df[column].unique()
        columns = list(none_dict.keys())
    df.drop(columns, axis=1, inplace=True)
    return df


def drop_non_tz_rows(df):
    non_tz_stations = ['PACZ', 'PASN', 'PADK', 'PMDY', 'PASY', 'PHLU', 'KQT9',
                      'KEGC', 'KH78', 'KATP', 'KGBK', 'PHLI', 'KGRY', 'PHHI',
                      'KXCN', 'PACD', 'PHHN', 'K25T', 'PAOU', 'PAUT', 'KHHV',
                      'PHJR', 'PAKF', 'K28K','KIKT','PHOG','PAPB','PHNY','PHKO',
                      'PHJH','PADU','KGHB','PALU']
    df = df[~df['obs_id'].isin(non_tz_stations)]
    return df


def lookup_timezone(station):
    with open('/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/metar_timezone_dict.p', 'rb') as f:
        metar_timezone_dict = pickle.load(f, encoding='latin1')
    return metar_timezone_dict[station][1]


def lookup_lat_lon(station):
    with open("/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/metar_dict.p",'rb') as f:
        metar_dict = pickle.load(f, encoding='latin1')
    return metar_dict[station][0], metar_dict[station][1]

def add_solar_angle(df):
    for i in df.index:
        obs_id = df.loc[i, 'obs_id']
        latitude, longitude = lookup_lat_lon(obs_id)
        local_tz = lookup_timezone(obs_id)
        naive_gmt_time = df.loc[i, 'valid_time_gmt']
        prev_naive_gmt_time = df.loc[i, 'prev_valid_time_gmt']

        gmt_timestamp = datetime.datetime.fromtimestamp(naive_gmt_time)
        prev_gmt_timestamp = datetime.datetime.fromtimestamp(prev_naive_gmt_time)

#         local_time = local_tz.localize(gmt_timestamp)
#         prev_local_time = local_tz.localize(prev_gmt_timestamp)
        local_time = local_tz.fromutc(gmt_timestamp)
        prev_local_time = local_tz.fromutc(prev_gmt_timestamp)



        solar_angle = get_altitude(latitude, longitude, local_time)
        prev_solar_angle = get_altitude(latitude, longitude, prev_local_time)

        print(gmt_timestamp)
        print(local_time)
        print(solar_angle)

        df.loc[i, 'solar_angle'] = solar_angle
        df.loc[i, 'prev_solar_angle'] = prev_solar_angle

    return df

def pickle_df(df, filename):
    filename = "/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/" + filename + ".p"
    pd.to_pickle(df, filename)

def drop_times_icons_names(df):
    # do I want to drop key?
    columns_to_drop = ['expire_time_gmt', u'wx_icon', u'obs_id', u'valid_time_gmt', u'obs_name',
                      u'key', 'prev_expire_time_gmt', u'prev_wx_icon', u'prev_obs_id', u'prev_valid_time_gmt',
                        u'prev_obs_name', u'prev_key', 'day_ind', 'icon_extd', 'prev_day_ind', 'prev_icon_extd']
    return df


def drop_identifiers(df):
    columns_to_drop = ['_id', 'url']
    return df


def fill_missing_values(df):
    return df


def add_dummies(df):
    return df
