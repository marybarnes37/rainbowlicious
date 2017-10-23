import json
from pymongo import MongoClient
import os
import requests
import time
from PIL import Image
import io
import urllib
import pickle
import psutil
import subprocess
import re

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

# def get_api_key():
#     path = os.path.join(os.environ['HOME'],'flickr.txt')
#     #with open('~/flickr.txt', 'rb') as f:
#     with open(path,'rb') as f:
#         api_key = f.readline().strip()
#         secret = f.readline().strip()
#     return api_key, secret

def get_api_key():
    with open('/Users/marybarnes/.ssh/flickr.txt', 'rb') as f:
        api_key = f.readline().strip()
        secret = f.readline().strip()
    return api_key, secret

def dl_and_create_dict_radius(num_pages=4, search_term='rainbow', lat=47.60, lon=-122.33):
    api_key, secret_api = get_api_key()
    api_url = 'https://api.flickr.com/services/rest'
    for i in range(1, num_pages+1):
        params = {'method':'flickr.photos.search',
                      'api_key':api_key,
                      'perpage':100,
                      'format':'json',
                      'text': search_term,
                      'lat': lat,
                      'lon': lon,
                      'radius': 32,
                      'has_geo': 1,
                      'min_taken_date': 1350172800,
                      'max_taken_date': 1507939200,
                      'nojsoncallback':1,
                      'page': i}
        r = requests.get(api_url, params=params)
        radially_bound_rainbow_json = r.json()
        for j in range(250):
            try:
                photo_id = radially_bound_rainbow_json['photos']['photo'][j]['id']
                server = radially_bound_rainbow_json['photos']['photo'][j]['server']
                secret = radially_bound_rainbow_json['photos']['photo'][j]['secret']
                farm = radially_bound_rainbow_json['photos']['photo'][j]['farm']
                photo_url = 'https://farm{}.staticflickr.com/{}/{}_{}_m.jpg'.format(farm, server, photo_id, secret)
                photo_filename = '/Users/marybarnes/capstone_galvanize/seattle_radial_photos/{}'.format(photo_id)
                urllib.urlretrieve(photo_url, photo_filename )
                photo_dict[photo_id] = [farm, server, photo_id, secret]
            except:
                break
    with open("sea_radial_photodict_precheck.pkl",'wb') as f:
        pickle.dump(photo_dict, f)

def dl_and_create_dict_text(collection, num_pages=93, search_term='seattle rainbow'):
    api_key, secret_api = get_api_key()
    api_url = 'https://api.flickr.com/services/rest'
    photo_dict = {}
    for i in range(1, num_pages+1):
        params = {'method':'flickr.photos.search',
                  'api_key':api_key,
                  'format':'json',
                  'text': search_term,
                  'min_taken_date': 1350172800,
                  'max_taken_date': 1507939200,
                  'nojsoncallback':1,
                  'sort' : 'relevance',
                  'page': i}
        r = requests.get(api_url, params=params)
        strings = ['ainbow', 'cloud', 'over']
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
                    urllib.urlretrieve(photo_url, photo_filename)
                    photo_dict[photo_id] = [farm, server, photo_id, secret, order]
                    collection.insert_one({'raw_json': json, 'relevance_order' : order})
                if j % 10 == 0:
                    total = collection.find().count()
                    print("at {}_{} iterations have added {} documents to the collection".format(i,j, total))
            except:
                break
            time.sleep(2)
    with open("sea_text_photodict_precheck.pkl",'wb') as f:
        pickle.dump(photo_dict, f)


def label_photos(collection, num_pages=93):
    counter = 0
    skip_count = 0
    for i in range(1, num_pages+1):
        # { "$regex" : '/^{}_/'.format(i) }
        cursor = collection.find({ "label" : { "$exists" : False }, "date_check" : { "$exists" : True },
                                    "relevance_order": { "$regex" : '^{}\_'.format(i) }}, no_cursor_timeout=True)
        print(cursor.count())

        for record in cursor:
            try:
                photo_filename = '/Users/marybarnes/capstone_galvanize/seattle_text_photos/{}_{}.jpg'.format(record['relevance_order'], record['raw_json']['id'])
                img = Image.open(photo_filename)
            except:
                print('missing photo is {}_{}.jpg'.format(record['relevance_order'], record['raw_json']['id']))
                skip_count += 1
                print('skipcount is {}'.format(skip_count))
                continue
            img.show()
            collection.update_one({"_id": record["_id"]}, {"$set": {'label': raw_input("Enter 1 for rainbow or 0 for not rainbow: ")}})
            img.close()
            os.system('pkill Preview') # find a way to close image first, or do this with matplotlib
            counter +=1
            if counter == 4:
                break

def get_photo_info(collection):
    api_key, secret = get_api_key()
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle', address='mongodb://localhost:27017/')
    cursor = collection.find({ "raw_json" : { "$exists" : True }, "photo_info" : { "$exists" : False }}, no_cursor_timeout=True)
    api_url = 'https://api.flickr.com/services/rest'
    counter = 0
    skip_counter = 0
    for record in cursor:
        photo_id = record['raw_json']['id']
        secret = record['raw_json']['secret']
        params = {'method':'flickr.photos.getInfo',
                  'api_key':api_key,
                  'id': photo_id,
                  'secret': secret,
                  'format':'json',
                  'nojsoncallback':1}
        r = requests.get(api_url, params=params)
        if r.status_code == 200 and 'photo' in r.json():
            counter += 1
            collection.update_one({"_id": record["_id"]}, {"$set": {'photo_info': r.json()}})
        else:
            skip_counter += 1
            print("SKIPPING: status code: {}\n {} \n{}".format(r.status_code, r.content, r.headers))
        total = collection.find().count()
        print("{} added this round for a total of {} documents in collection ({} skipped)".format(counter, total, skip_counter))
        time.sleep(3)
    client.close()



def remove_unknown_added_dates(collection):
    api_key, secret = get_api_key()
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle', address='mongodb://localhost:27017/')
    cursor = collection.find( {"photo_info" : { "$exists" : True },  "date_check" : { "$exists" : False }}, no_cursor_timeout=True)
    client.close()
    pass




def main():
    api_key, secret = get_api_key()
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle', address='mongodb://localhost:27017/')
    # dl_and_create_dict_text(collection)
    get_photo_info(collection)
    # label_photos(collection)
    # remove_unknown_added_dates(collection)
    client.close()
