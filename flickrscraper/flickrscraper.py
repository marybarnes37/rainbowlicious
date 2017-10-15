import json
from pymongo import MongoClient
import os
import requests
import time

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


def get_flickr_json(collection, api_key, group_ids, pages):
    url = 'https://api.flickr.com/services/rest'
    for i in range(len(group_ids)):
        for j in range(pages[i]):
            params = {'method':'flickr.groups.pools.getPhotos',
                      'api_key':api_key, 'group_id':group_ids[i],
                      'extras':'date_taken,geo', 'perpage':100,
                     'format':'json', 'nojsoncallback':1,
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
    print(api_key)
    print(len(api_key))
    # client, collection = setup_mongo_client('capstone', 'flickr_rainbow', address='mongodb://localhost:27017/')
    # group_ids = ['52241461495@N01', '62702064@N00']
    # pages = [119, 178]
    # get_flickr_json(collection, api_key, group_ids, pages)
    # client.close()
