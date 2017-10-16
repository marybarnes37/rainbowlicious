from pymongo import MongoClient
import pandas as pd
import reverse_geocoder
import pickle

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
    df_flickr = pd.DataFrame(columns=['id', 'dateadded', 'datetaken', 'datetakenunknown', 'longitude_exact', 'latitude_exact'])
    for i in range(collection.find().count()):
        for photo in cursor[i]['photos']['photo']:
            df_flickr = df_flickr.append(pd.DataFrame(
                        [[photo['id'], photo['dateadded'], photo['datetaken'], photo['datetakenunknown'], photo['longitude'],
                          photo['latitude']]],  columns=['id', 'dateadded', 'datetaken',
                                                         'datetakenunknown', 'longitude_exact', 'latitude_exact']), ignore_index=True)
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
    return df_test.dropna()


def pickle_df(df, file_name):
    with open('pickles/flickr_rain.pkl', 'wb') as f:
        pickle.dump(df, f)


def main(client_text='capstone', collection_text='flickr_rainbow'):
    client, collection = setup_mongo_client(client_text, collection_text)
    df = flickr_dataframe(collection)
    df = drop_unknown_dates(df)
    df = keep_US_locations(df)
    df = clean_flickr_df(df)
    pickle_df(df, collection_text)
