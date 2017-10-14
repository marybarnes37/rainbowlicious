import json
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
    with open('$HOME/flickr.txt', 'rb') as f:
        api_key = f.readline()
        secret = f.readline()
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
            if r.status_code != 200:
                with open('/Users/marybarnes/capstone_galvanize/rainbowlicious/flickrscraper/status_code_log.txt', "a") as myfile:
                    myfile.write("status code: {}\n {} \n{}".format(r.status_code, r.content, r.headers))
                time.sleep(60)
            else:
                collection.insert_one(r.json())
            time.sleep(10)


def main():
    api_key, secret = get_api_key()
    client, collection = setup_mongo_client('capstone', 'insta_rainbow', address='mongodb://localhost:27017/')
    group_ids = ['52241461495@N01', '62702064@N00']
    pages = [119, 178]
    get_flickr_json(collection, api_key, group_ids, pages)
