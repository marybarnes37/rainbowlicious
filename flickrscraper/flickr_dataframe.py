from pymongo import MongoClient
import pandas as pd
import reverse_geocoder
import pickle
import time

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
    return df.dropna()


def pickle_df(df, file_name):
    with open('pickles/{}'.format(file_name), 'wb') as f:
        pickle.dump(df, f)


def main(client_text='capstone', collection_text='flickr_rainbow_correct'):
    client, collection = setup_mongo_client(client_text, collection_text)
    df = flickr_dataframe(collection)
    print('finished creating dataframe')
    pickle_df(df, 'flickr_stage1')
    df = drop_unknown_dates(df)
    print('finished dropping unknown dates')
    pickle_df(df, 'flickr_stage2')
    df = keep_US_locations(df)
    print('finished US locations')
    pickle_df(df, 'flickr_stage3')
    df = clean_flickr_df(df)
    print('finished cleaning')
    pickle_df(df, collection_text)
    client.close()
