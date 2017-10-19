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
                    print('added to dict')
                    collection.insert_one({'raw_json': json, 'relevance_order' : order})
                    print('added to mongo')
                if j % 10 == 0:
                    total = collection.find().count()
                    print("at {}_{} iterations have added {} documents to the collection".format(i,j, total))
            except:
                break
            time.sleep(2)
    with open("sea_text_photodict_precheck.pkl",'wb') as f:
        pickle.dump(photo_dict, f)


def label_photos(pickled_file_name="sea_radial_photodict_precheck.pkl"):
    with open(pickled_file_name, "rb") as f:
        photo_dict = pickle.load(f)
    counter = 0
    for photo_id in photo_dict.iterkeys():
        photo_filename = '/Users/marybarnes/capstone_galvanize/seattle_radial_photos/{}'.format(photo_id)
        img = Image.open(photo_filename)
        img.show()
        photo_dict[photo_id].append(raw_input("Enter 1 for rainbow or 0 for not rainbow: "))
        img.close()
        os.system('pkill Preview')
        counter +=1
        if counter == 4:
            break
    with open("verified_sea_radial_photodict.pkl",'wb') as f:
        pickle.dump(photo_dict, f)


def get_flickr_json(collection, api_key, group_ids, pages):
    url = 'https://api.flickr.com/services/rest'
    for j in range():
        params = {'method':'flickr.photos.search',
                  'api_key':api_key,
                  'perpage':100,
                  'format':'json',
                  'lat': '47.606',
                  'lon': '-122.332',
                  'radius': '48',
                  'has_geo': '1',
                  'geo_context': '2' , # try without this too
                  'min_taken_date': '1350172800',
                  'max_taken_date': '1507939200',
                  'nojsoncallback':1,
                  'page':j}
        r = requests.get(url, params=params)
        if r.status_code == 200:
            collection.insert_one(r.json())
            print("gathered page {}".format(j))
            time.sleep(7)
        else:
            with open('/Users/marybarnes/capstone_galvanize/rainbowlicious/flickrscraper/status_code_log.txt', "a") as myfile:
                myfile.write("status code: {}\n {} \n{}".format(r.status_code, r.content, r.headers))
            print('encountered status code {}'.format(r.status_code))
            return None
        if j % 10 == 0:
            total = collection.find().count()
            print("added {} documents to the collection".format(total))
    print("gathered all of group id {}".format(group_ids[i]))
    total = collection.find().count()
    print('finished gathering a total of {} documents'.format(total))


def main():
    api_key, secret = get_api_key()
    client, collection = setup_mongo_client('capstone', 'flickr_rainbow_seattle', address='mongodb://localhost:27017/')
    dl_and_create_dict_text(collection)
    # get_flickr_json(collection, api_key, group_ids, pages)
    client.close()
