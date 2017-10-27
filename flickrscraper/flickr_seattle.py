import json
from pymongo import MongoClient
import os
import requests
import time
from PIL import Image
import io
import pandas as pd
import urllib
import pickle
import psutil
import subprocess
import re
from bson.objectid import ObjectId
from pysolar.solar import get_altitude
import datetime
import calendar
import pytz
from tzwhere import tzwhere
import os
import datetime
# from StringIO import StringIO
from io import StringIO, BytesIO



# for getting missing images ls -la | grep 172B | cut -c57- | perl -ne '/.*_(.*).jpg/; print "$1\n"'


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
    path = os.path.join(os.environ['HOME'],'flickr.txt')
    #with open('~/flickr.txt', 'rb') as f:
    with open(path,'rb') as f:
        api_key = f.readline().strip()
        secret = f.readline().strip()
    return api_key, secret

# def get_api_key():
#     with open('/Users/marybarnes/.ssh/flickr.txt', 'rb') as f:
#         api_key = f.readline().strip()
#         secret = f.readline().strip()
#     return api_key, secret


def view_duplicate_timestamps(time):
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle_w_dates', address='mongodb://localhost:27017/')
    cursor = collection.find({'raw_json.datetaken': time})
    for record in cursor:
        print(record)
    client.close()


def delete_field(field, collection):
    collection.update_many({}, {'$unset': {field: ''}})


def delete_fields(fields = ['duplicate', 'pride_day', 'snoqualmie', 'raw_json.solar_angle', 'bad_solar_angle']):
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle_w_dates')
    for field in fields:
        delete_field(field, collection)
    client.close()

def dl_and_create_dict_text(num_pages=93, search_term='seattle rainbow'):
    api_key, secret = get_api_key()
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle_w_dates', address='mongodb://localhost:27017/')
    api_url = 'https://api.flickr.com/services/rest'
    photo_dict = {}
    skipped_counter = 0
    page_num = 7
    for i in range(1, num_pages+1):
        params = {'method':'flickr.photos.search',
                  'api_key':api_key,
                  'format':'json',
                  'text': search_term,
                #   'max_taken_date': 1350172800,
                  'extras': 'date_taken',
                  'nojsoncallback':1,
                  'sort' : 'relevance',
                  'page': page_num}
        r = requests.get(api_url, params=params)
        strings = ['ainbow', 'cloud', 'over', 'ain', 'windy', 'storm']
        page_num += 1
        for j in range(100):
            try:
                json = r.json()['photos']['photo'][j]
                title = json['title']
                if any(x in title for x in strings):
                    order = "{}_{}".format(i, j)
                    photo_id = json['id']
                    server = json['server']
                    secret = json['secret']
                    farm = json['farm']
                    photo_url = 'https://farm{}.staticflickr.com/{}/{}_{}_m.jpg'.format(farm, server, photo_id, secret)
                    photo_filename  = os.path.join(os.environ['HOME'], 'seattle_text_photos/{}_{}.jpg'.format(order, photo_id))
                    r_photo = requests.get(photo_url)
                    if r_photo.status_code == 200:
                        img = Image.open(StringIO(r_photo.content))
                        img.save(photo_filename)
                        photo_dict[photo_id] = [farm, server, photo_id, secret, order]
                        collection.insert_one({'raw_json': json, 'relevance_order':order})
                    else:
                        print(photo_url)
                        print(r_photo.status_code)
                        print(r_photo.content)
                        skipped_counter += 1
                        continue
                    # urllib.urlretrieve(photo_url, photo_filename)
                if j % 10 == 0:
                    total = collection.find().count()
                    print("at {}_{} iterations have added {} documents to the collection".format(i,j, total))
                    print("{} images have been skipped".format(skipped_counter))
            except:
                break
            time.sleep(5)
    with open("sea_text_photodict_precheck_second_batch.pkl",'wb') as f:
        pickle.dump(photo_dict, f)

def download_missing_images():
    client, collection = setup_mongo_client('capstone',   'flickr_rainbow_seattle_w_dates', address='mongodb://localhost:27017/')
    skipped_counter = 0
    with open('/Users/marybarnes/capstone_galvanize/missing_images_ids.txt', 'rb') as f:
        missing_img_ids = pickle.load(f)
    for img_id in missing_img_ids:
        record = collection.find_one( {"raw_json.id" : img_id}, no_cursor_timeout=True)
        order = record['relevance_order']
        photo_id = img_id
        server = record['raw_json']['server']
        secret = record['raw_json']['secret']
        farm = record['raw_json']['farm']
        photo_url = 'https://farm{}.staticflickr.com/{}/{}_{}_m.jpg'.format(farm, server, photo_id, secret)
        photo_filename  = '/Users/marybarnes/capstone_galvanize/missing_photos/{}_{}.jpg'.format(order, photo_id)
        r_photo = requests.get(photo_url)
        if r_photo.status_code == 200:
            img = Image.open(BytesIO(r_photo.content))
            img.save(photo_filename)
        else:
            print(photo_url)
            print(r_photo.status_code)
            print(r_photo.content)
            skipped_counter += 1
            print('skipped {}'.format(skipped_counter))
            continue
        time.sleep(3)

def mark_unknown_dates():
    client, collection = setup_mongo_client('capstone',   'flickr_rainbow_seattle_w_dates', address='mongodb://localhost:27017/')
    cursor = collection.find( {"raw_json" : { "$exists" : True },  "unknown_date" : { "$exists" : False }}, no_cursor_timeout=True)
    one_counter = 0
    zero_counter = 0
    for record in cursor:
        if record['raw_json']['datetakenunknown'] == 1:
            collection.update_one({"_id": record["_id"]}, {"$set": {'unknown_date': 1}})
            one_counter += 1
        else:
            collection.update_one({"_id": record["_id"]}, {"$set": {'unknown_date': 0}})
            zero_counter += 1
        print("one_counter (bad): {}; zero_counter (good): {}".format(one_counter, zero_counter))
    total = collection.find({"unknown_date": 0}).count()
    print("{} viable documents".format(total))
    client.close()



def create_dataframe_to_check_duplicates(collection):
    cursor = collection.find({"unknown_date": 0}, no_cursor_timeout=True)
    col = ['_id', 'local_datetime_taken']
    df = pd.DataFrame(columns=col)
    for record in cursor:
        df = df.append(pd.DataFrame([[record['_id'], record['raw_json']['datetaken']]],  columns=col), ignore_index=True)
    cursor.close()
    pattern = '%Y-%m-%d %H:%M:%S'
    for i in df.index:
        local_epoch = int(time.mktime(time.strptime(df.loc[i, 'local_datetime_taken' ], pattern)))
        df.loc[i, 'local_epoch'] = local_epoch
    df = df.sort_values(['local_epoch'])
    pd.to_pickle(df, "/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/flickr_seattle_datetimes_sorted_all_new.p")
    return df


def create_duplicates_list(duplicates_filename = '/Users/marybarnes/capstone_galvanize/flickr_seattle_duplicates_all_new_2.txt',
                            non_duplicates_filename = '/Users/marybarnes/capstone_galvanize/flickr_seattle_duplicates_with_counts.txt',
                    pickled_df = "/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/flickr_seattle_datetimes_sorted_all_new.p"):
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle_w_dates')
    if pickled_df:
        df = pd.read_pickle(pickled_df)
    else:
        df = create_dataframe_to_check_duplicates(collection)
    f = open(duplicates_filename, 'w')
    f2 = open(non_duplicates_filename, 'w')
    previous_epoch = 0
    count = 1
    count_dict = {}
    index_of_previous_non_duplicate = 0
    for i in df.index:
        obs_epoch = df.loc[i, 'local_epoch']
        if abs(obs_epoch - previous_epoch) < 1800:
            count += 1
            f.write(str(df.loc[i, '_id']) + '\n')
            f2.write(str(df.loc[i, '_id']) + ' 1 ' + str(count) + '\n')
            df.loc[i, 'dup_count'] = count
        else:
            count_dict[df.loc[index_of_previous_non_duplicate, '_id']] = count
            f2.write(str(df.loc[index_of_previous_non_duplicate, '_id']) + ' 0 ' + str(count) + '\n')
            previous_epoch = obs_epoch
            count = 1
            index_of_previous_non_duplicate = i
    with open("/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/duplicates_count_dict.pkl",'wb') as f:
       pickle.dump(count_dict, f)
    f.close()
    f2.close()
    pd.to_pickle(df, "/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/flickr_seattle_datetimes_sorted_with_counts.p")
    client.close()



def mark_duplicates(duplicates_filename = '/Users/marybarnes/capstone_galvanize/flickr_seattle_duplicates_all_new.txt'):
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle_w_dates')
    with open(duplicates_filename, 'r') as f:
        for _id in f:
            collection.update_one({"_id": ObjectId(_id.strip())}, {"$set": {"duplicate_new": 1}}, upsert=False)
    client.close()


# def mark_non_duplicates():
#     client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle_w_dates')
#     collection.update_many({"unknown_date": 0, "duplicate": {"$exists" : False}}, {"$set": {"duplicate": 0}})
#     total_dups = collection.find({"duplicate": 1}).count()
#     total_non_dups = collection.find({"duplicate": 0}).count()
#     print("total dups: {}; total_non_dups: {}".format(total_dups, total_non_dups))
#     client.close()

def add_duplicate_counts(duplicate_counts_filename = '/Users/marybarnes/capstone_galvanize/flickr_seattle_duplicates_with_counts.txt'):
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle_w_dates')
    with open(duplicate_counts_filename, 'r') as f:
        for line in f:
            collection.update_one({"_id": ObjectId(line[:24].strip())}, {"$set": {"duplicate_new": line[25:26]}}, upsert=False)
            collection.update_one({"_id": ObjectId(line[:24].strip())}, {"$set": {"duplicate_count" : line[27:]}}, upsert=False)
    client.close()

def view_duplicates(duplicate_counts_filename = '/Users/marybarnes/capstone_galvanize/flickr_seattle_duplicates_with_counts.txt'):
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle_w_dates')
    with open(duplicate_counts_filename, 'r') as f:
        for line in f:
            if int(line[27:]) == 2:
                print("\n\n\nSTARTING new group:")
            if int(line[27:]) >= 1:
                record = collection.find_one({"_id": ObjectId(line[:24].strip())})
                print("time: {}".format(record['raw_json']['datetaken']))
                photo_id = record['raw_json']['id']
                server = record['raw_json']['server']
                secret = record['raw_json']['secret']
                farm = record['raw_json']['farm']
                print('https://farm{}.staticflickr.com/{}/{}_{}_m.jpg'.format(farm, server, photo_id, secret))
                print('relevance order (page_orderOnPage): {}'.format(record['relevance_order']))
                print('\n')
    client.close



def mark_pride():
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle_w_dates', address='mongodb://localhost:27017/')
    cursor = collection.find( {"duplicate" : 0, "pride_day" : { "$exists" : False } }, no_cursor_timeout=True)
    zero_counter = 0
    one_counter = 0
    for record in cursor:
        if 'pride' in record['raw_json']['title']:
            collection.update_one({"_id": record["_id"]}, {"$set": {'pride_day': 1}})
            one_counter += 1
        else:
            collection.update_one({"_id": record["_id"]}, {"$set": {'pride_day': 0}})
            zero_counter += 1
        print("one_counter (bad): {}; zero_counter (good): {}".format(one_counter, zero_counter))
    total = collection.find({"pride_day": 0}).count()
    print("{} viable documents".format(total))
    client.close()


def mark_snoqualmie():
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle_w_dates', address='mongodb://localhost:27017/')
    cursor = collection.find( {"pride_day" : 0, "snoqualmie" : { "$exists" : False } }, no_cursor_timeout=True)
    zero_counter = 0
    one_counter = 0
    for record in cursor:
        if 'waterfall' in record['raw_json']['title']:
            collection.update_one({"_id": record["_id"]}, {"$set": {'snoqualmie': 1}})
            one_counter += 1
        else:
            collection.update_one({"_id": record["_id"]}, {"$set": {'snoqualmie': 0}})
            zero_counter += 1
        print("one_counter (bad): {}; zero_counter (good): {}".format(one_counter, zero_counter))
    total = collection.find({"snoqualmie": 0}).count()
    print("{} viable documents".format(total))
    client.close()


def lookup_timezone(station):
    with open('/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/metar_timezone_dict.p', 'rb') as f:
        metar_timezone_dict = pickle.load(f, encoding='latin1')
    return metar_timezone_dict[station][1]


def lookup_lat_lon(station):
    with open("/Users/marybarnes/capstone_galvanize/rainbowlicious/pickles/metar_dict.p",'rb') as f:
        metar_dict = pickle.load(f, encoding='latin1')
    return metar_dict[station][0], metar_dict[station][1]


def add_solar_angle(station="KBFI"):
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle_w_dates', address='mongodb://localhost:27017/')
    local_tz = lookup_timezone(station)
    latitude, longitude = lookup_lat_lon(station)
    cursor = collection.find( {"snoqualmie" : 0}, no_cursor_timeout=True)
    pattern = '%Y-%m-%d %H:%M:%S'
    added_counter = 0
    for record in cursor:
        string_time = record['raw_json']['datetaken']
        dt_obj = datetime.datetime.strptime(string_time, pattern)
        date_time =  local_tz.localize(dt_obj)
        solar_angle = get_altitude(latitude, longitude, date_time)
        print(string_time)
        print(date_time)
        print(solar_angle)
        print("\n")
        collection.update_one({"_id": record["_id"]}, {"$set": {"raw_json.solar_angle": solar_angle}})
        added_counter += 1
        print("added {}".format(added_counter))
    client.close()


def mark_bad_solar_angle():
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle_w_dates', address='mongodb://localhost:27017/')
    cursor = collection.find( {"raw_json.solar_angle" : { "$exists" : True }, "bad_solar_angle": { "$exists" : False } }, no_cursor_timeout=True)
    zero_counter = 0
    one_counter = 0
    for record in cursor:
        if (record['raw_json']['solar_angle'] < -2) or (record['raw_json']['solar_angle'] > 44):
            collection.update_one({"_id": record["_id"]}, {"$set": {'bad_solar_angle': 1}})
            one_counter += 1
        else:
            collection.update_one({"_id": record["_id"]}, {"$set": {'bad_solar_angle': 0}})
            zero_counter += 1
        print("one_counter (bad): {}; zero_counter (good): {}".format(one_counter, zero_counter))
    total = collection.find({"bad_solar_angle": 0}).count()
    print("{} viable documents".format(total))
    client.close()


def label_photos(num_pages=200):
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle_w_dates', address='mongodb://localhost:27017/')
    skip_count = 0
    for i in range(1, num_pages+1):
        # { "$regex" : '/^{}_/'.format(i) }
        # change the solar angle and label those as well
        cursor = collection.find({ "label" : { "$exists" : False }, "bad_solar_angle" : 1,
                                    "relevance_order": { "$regex" : '^{}\_'.format(i) }}, no_cursor_timeout=True)
        # cursor = collection.find({ "label" : { "$nin" : ['0', 0, '1', 1, '2', 2, '-'] }, "bad_solar_angle" : 0}, no_cursor_timeout=True)
        print(cursor.count())
        for record in cursor:
            try:
                photo_filename = '/Users/marybarnes/capstone_galvanize/seattle_text_photos/{}_{}.jpg'.format(record['relevance_order'], record['raw_json']['id'])
                print('opening image')
                img = Image.open(photo_filename)
            except Exception as e:
                print(e)
                print('missing photo is {}'.format(photo_filename))
                skip_count += 1
                print('skipcount is {}'.format(skip_count))
                continue
            print('showing image')
            img.show()
            collection.update_one({"_id": record["_id"]}, {"$set": {'label': input("Enter 1 for rainbow or 0 for not rainbow: ")}})
            img.close()
            os.system('pkill Preview')



def add_weather_data():
    pass

def main():
    # api_key, secret = get_api_key()
    # client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle_w_dates', address='mongodb://localhost:27017/')
    # dl_and_create_dict_text(collection)
    # mark_unknown_dates()
    # create_duplicates_list(pickled_df=None)
    mark_duplicates()
    mark_non_duplicates()
    mark_pride()
    mark_snoqualmie()
    add_solar_angle()
    mark_bad_solar_angle()
    # label_photos()
    # client.close()



# def dl_and_create_dict_radius(num_pages=4, search_term='rainbow', lat=47.60, lon=-122.33):
#     api_key, secret_api = get_api_key()
#     api_url = 'https://api.flickr.com/services/rest'
#     for i in range(1, num_pages+1):
#         params = {'method':'flickr.photos.search',
#                       'api_key':api_key,
#                       'perpage':100,
#                       'format':'json',
#                       'text': search_term,
#                       'lat': lat,
#                       'lon': lon,
#                       'radius': 32,
#                       'has_geo': 1,
#                       'min_taken_date': 1350172800,
#                       'max_taken_date': 1507939200,
#                       'nojsoncallback':1,
#                       'page': i}
#         r = requests.get(api_url, params=params)
#         radially_bound_rainbow_json = r.json()
#         for j in range(250):
#             try:
#                 photo_id = radially_bound_rainbow_json['photos']['photo'][j]['id']
#                 server = radially_bound_rainbow_json['photos']['photo'][j]['server']
#                 secret = radially_bound_rainbow_json['photos']['photo'][j]['secret']
#                 farm = radially_bound_rainbow_json['photos']['photo'][j]['farm']
#                 photo_url = 'https://farm{}.staticflickr.com/{}/{}_{}_m.jpg'.format(farm, server, photo_id, secret)
#                 photo_filename = '/Users/marybarnes/capstone_galvanize/seattle_radial_photos/{}'.format(photo_id)
#                 urllib.urlretrieve(photo_url, photo_filename )
#                 photo_dict[photo_id] = [farm, server, photo_id, secret]
#             except:
#                 break
#     with open("sea_radial_photodict_precheck.pkl",'wb') as f:
#         pickle.dump(photo_dict, f)

# def get_photo_info():
#     api_key, secret = get_api_key()
#     client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle', address='mongodb://localhost:27017/')
#     cursor = collection.find({ "raw_json" : { "$exists" : True }, "photo_info" : { "$exists" : False }}, no_cursor_timeout=True)
#     api_url = 'https://api.flickr.com/services/rest'
#     counter = 0
#     skip_counter = 0
#     for record in cursor:
#         photo_id = record['raw_json']['id'].strip()
#         secret = record['raw_json']['secret'].strip()
#         params = {'method':'flickr.photos.getInfo',
#                   'api_key':api_key,
#                   'id': int(photo_id),
#                   'secret': secret,
#                   'format':'json',
#                   'nojsoncallback':1}
#         r = requests.get(api_url, params=params)
#         if r.status_code == 200 and 'photo' in r.json():
#             counter += 1
#             collection.update_one({"_id": record["_id"]}, {"$set": {'photo_info': r.json()}})
#         else:
#             skip_counter += 1
#             print("photo_id: {}, secret: {}".format(photo_id, secret))
#             print("SKIPPING: status code: {}\n {} \n{}".format(r.status_code, r.content, r.headers))
#         total = collection.find().count()
#         print("{} added this round for a total of {} documents in collection ({} skipped)".format(counter, total, skip_counter))
#         time.sleep(3)
#     client.close()
