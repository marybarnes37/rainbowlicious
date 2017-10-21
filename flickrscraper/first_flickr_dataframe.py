from pymongo import MongoClient
import pandas as pd
import reverse_geocoder
import pickle
import time
import pytz
from tzwhere import tzwhere
from geopy.distance import vincenty
import datetime

def setup_mongo_client(db_name, collection_name, client=None, address='mongodb://localhost:27017/'):
    if not client:
        client = MongoClient(address)
    else:
        client = client
    db = client[db_name]
    collection = db[collection_name]
    return client, collection


def flickr_dataframe(collection):
    cursor = collection.find()
    col = ['id', 'dateadded', 'datetaken', 'datetakenunknown', 'longitude_exact', 'latitude_exact']
    df_flickr = pd.DataFrame(columns=col)
    for i in range(collection.find().count()):
        for photo in cursor[i]['photos']['photo']:
            df_flickr = df_flickr.append(pd.DataFrame(
                        [[photo['id'], photo['dateadded'], photo['datetaken'], photo['datetakenunknown'], photo['longitude'],
                          photo['latitude']]],  columns=col), ignore_index=True)
    cursor.close()
    return df_flickr


def drop_unknown_dates(df):
    df = df[df['datetakenunknown'] != '1']
    return df


def keep_US_locations(df):
    df = df[df['latitude_exact'] != 0]
    df.index = range(len(df))
    for i in range(df.shape[0]):
        location_data = reverse_geocoder.search((df.loc[i, 'latitude_exact'], df.loc[i, 'longitude_exact']))
        if location_data[0]['cc'] == 'US':
            for key in location_data[0].keys():
                df.loc[i, key] = location_data[0][key]
    return df


def clean_flickr_df(df):
    return df.dropna()



def find_closest_station(lat, lon):
    closest_station = None
    shortest_miles = 10000
    for key, value in metar_dict.iteritems():
        miles = vincenty((lat, lon), value).miles
        if miles < shortest_miles:
            shortest_miles = miles
            closest_station = key
    return closest_station, shortest_miles

def get_closest_stations(df):
    df['closest_station'] = None
    df['miles_to_station'] = None
    for i in df.index:
        station, miles = find_closest_station(df.loc[i, 'latitude_exact'], df.loc[i, 'longitude_exact'])
        df.loc[i, 'closest_station'] = station
        df.loc[i, 'miles_to_station'] = miles
    df = df[df['miles_to_station'] <= 30]
    df[df['closest_station'] != 'KMMM'] # mammoth falls, Yellowstone
    return df


def get_string_min_hour(num):
    if num < 10:
        return "0" + str(num)
    else:
        return str(num)

def fix_hours_days(hour, day):
    if hour == 24:
        hour = 0
        day += 1
    if hour == -1:
        hour = 23
        day -= 1
    return hour, day

def find_time_window(time):
    mm_orig = int(time[14:16])
    HH_orig = int(time[11:13])
    DD_orig = int(time[8:10])
    if mm_orig + 30 > 59:
        mm_start = mm_orig - 30
        HH_start = HH_orig
        HH_end = HH_orig +1
    if mm_orig + 30 <= 59:
        mm_start = mm_orig + 30
        HH_start = HH_orig - 1
        HH_end = HH_orig
    mm_end = mm_start + 3
    if mm_end > 60:
        mm_end -= 60
        HH_end += 1
    # fix change between days
    HH_end, DD_end = fix_hours_days(HH_end, DD_orig)
    HH_start, DD_start = fix_hours_days(HH_start, DD_orig)
    # get string version
    HH_start = get_string_min_hour(HH_start)
    mm_start = get_string_min_hour(mm_start)
    DD_start = get_string_min_hour(DD_start)
    HH_end = get_string_min_hour(HH_end)
    mm_end = get_string_min_hour(mm_end)
    DD_end = get_string_min_hour(DD_end)
    #create time window
    start = "".join([time[:4], time[5:7], DD_start, HH_start, mm_start])
    stop = "".join([time[:4], time[5:7], DD_end, HH_end, mm_end])
    return start, stop

def get_utc(time, lat, lon, tzwhere_obj):
    timezone_str = tzwhere_obj.tzNameAt(float(lat), float(lon))
    if timezone_str == None:
        timezone_str = tzwhere_obj.tzNameAt(float(lat)-3, float(lon)-3)
        if timezone_str == None:
            timezone_str = tzwhere_obj.tzNameAt(float(lat)+3, float(lon)+3)
            if timezone_str == None:
                timezone_str = tzwhere_obj.tzNameAt(float(lat)-3, float(lon)+3)
                if timezone_str == None:
                    timezone_str = tzwhere_obj.tzNameAt(float(lat)+3, float(lon)-3)
    local_tz = pytz.timezone(timezone_str)
    datetime_without_tz = datetime.datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    datetime_with_tz = local_tz.localize(datetime_without_tz, is_dst=None)
    datetime_in_utc = datetime_with_tz.astimezone(pytz.utc)
    string_utc = datetime_in_utc.strftime('%Y-%m-%d %H:%M:%S %Z')
    return string_utc


def add_time_window(df):
    df['utc_datetaken'] = ""
    df['YYYYMMDDHHmm_start'] = ""
    df['YYYYMMDDHHmm_end'] = ""
    tzwhere_obj = tzwhere.tzwhere()
    for i in df.index:
        try:
            df.loc[i, 'utc_datetaken'] = get_utc(df.loc[i, 'datetaken'], df.loc[i, 'lat'], df.loc[i, 'lon'], tzwhere_obj)
            start, stop = find_time_window(df.loc[i, 'utc_datetaken'])
            df.loc[i, 'YYYYMMDDHHmm_start'] = start
            df.loc[i, 'YYYYMMDDHHmm_end'] = stop
        except:
            df.drop(i, axis=0, inplace=True)
        print(df.shape)
    time.sleep(.5)
    return df


def pickle_df(df, filename):
    df.to_pickle("/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/{}.p".format(filename))

def main(client_text='capstone', collection_text='flickr_rainbow_correct'):
    # client, collection = setup_mongo_client(client_text, collection_text)
    # df = flickr_dataframe(collection)
    # print('finished creating dataframe')
    # pickle_df(df, 'flickr_stage1')
    # df = drop_unknown_dates(df)
    # print('finished dropping unknown dates')
    # pickle_df(df, 'flickr_stage2')
    # df = keep_US_locations(df)
    # print('finished US locations')
    # pickle_df(df, 'flickr_stage3')
    # df = clean_flickr_df(df)
    # print('finished cleaning')
    # df = get_closest_stations(df)
    # print('got closest stations')
    # pickle_df(df, 'flickr_stage4')
    df = pd.read_pickle("/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/old_pickles/flickr_with_stations.p")
    df = df[df['miles_to_station'] <= 30]
    df = df[df['closest_station'] != 'KMMM'] # yellowstone mammoth falls
    df = add_time_window(df)
    print('added utc time window')
    pickle_df(df, 'flickr_with_station_and_time')
    # client.close()
