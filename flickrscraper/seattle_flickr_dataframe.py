
def main(client_text='capstone', collection_text='flickr_rainbow_seattle'):
    client, collection = setup_mongo_client(client_text, collection_text)
    df = flickr_dataframe(collection)
    print('finished creating dataframe')
