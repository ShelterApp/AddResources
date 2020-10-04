import json
import pandas as pd
from pymongo import MongoClient, TEXT
import random
import os
from tqdm import tqdm

# set the DB user name and password in config
with open('config.json', 'r') as con:
  config = json.load(con)

# Establish global variables
client = MongoClient(
    "mongodb+srv://" + config['userId'] + ":" + config['password'] 
    + "@shelter-rm3lc.azure.mongodb.net/shelter?retryWrites=true&w=majority"
)
dataset = 'shelter'


def make_ngrams(name, min_size=7):
    """Convert service name into list of n-grams.

    Args:
        name (str): the name of the service, e.g. 'FRIENDS OF LAKE HOPE'
        min_size (int, optional): the minimum number of characters in one ngram. Defaults to 7.

    Returns:
        list: list of ngram strings.
    """    
    length = len(name)
    size_range = range(min_size, max(length, min_size) + 1)
    return list(set(
        name[i:i + size]
        for size in size_range
        for i in range(0, max(0, length - size) + 1)
    ))


def refresh_ngrams(client, collection):
    """Make sure all the services in the desired collection have an ngram field.
       Also ensures that the n-gram field is included in the text index for the purpose of searching.

    Args:
        client (obj): pymongo MongoClient object
        collection (str): name of the collection in the db
    """    
    coll = client[dataset][collection]
    for document in coll.find():
        coll.update_one(
            {"_id": document["_id"]}, 
            {"$set": {
                "ngrams": make_ngrams(document["name"])
            }
            }
        )
    if 'ngrams_text' not in coll.index_information.keys():  # Check that the ngram field is a text index
        coll.create_index([("ngrams", TEXT)])


def distance(a,b):
    """ Calculates the Levenshtein distance between a and b.
    """
    n, m = len(a), len(b)
    if n > m:
        # Make sure n <= m, to use O(min(n,m)) space
        a,b = b,a
        n,m = m,n

    current = range(n+1)
    for i in range(1,m+1):
        previous, current = current, [i]+[0]*n
        for j in range(1,n+1):
            add, delete = previous[j]+1, current[j-1]+1
            change = previous[j-1]
            if a[j-1] != b[i-1]:
                change = change + 1
            current[j] = min(add, delete, change)

    return 1 - (current[n]/len(a))


def check_similarity(new_service, existing_service):
    return distance(new_service, existing_service) >= 0.9


def insert_services(data, client, collection):
    """Intake scraped services that have been processed and dupe-checked, and add to MongoDB.

    Args:
        data (dict): dictionary of IRS services containing ID, name, location and NTEE code
        client (obj): MongoClient object
        collection (str): the Mongo collection in which the data should be inserted
    """    
    db = client[dataset]
    db_coll = db[collection]
    db_coll.insert_many(data)


def grab_data(config, code_dict):
    """Access hosted IRS csv files, append them together, clean things up a bit and add code descriptions.

    Args:
        config (dict): config object imported from config.json
        code_dict (dict): dictionary containing NTEE codes and their descriptions

    Returns:
        DataFrame: Pandas DataFrame containing processed data
    """    
    urls = config['dataURLs'].values()
    codes = '|'.join(config['NTEE_codes'].keys())
    df = pd.DataFrame()
    for u in urls:
        print(f'grabbing {u}')
        response = pd.read_csv(u, usecols=[
            'EIN', 'NAME', 'STREET', 'CITY', 'STATE', 'ZIP', 'NTEE_CD'
        ]).fillna('0')
        print(f'initial shape: {response.shape}')
        response = response[response['NTEE_CD'].str.contains(codes)]  # filter for desired NTEE codes
        response = response[~response['STATE'].isin(config['military_mail_locs'])]  # filter out military mail locs
        print(list(set(list(response['STATE']))))
        response['ZIP'] = response['ZIP'].str.slice(start=0, stop=5)  # truncate ZIP codes
        print(f'final shape: {response.shape}')
        df = df.append(response, ignore_index=True)
    code_descriptions = []
    code_types = []
    code_subtypes = []
    for i in tqdm(range(len(df))):
        try:
            c = df.loc[i,'NTEE_CD']
            code_descriptions.append(code_dict[c]['service_summary'])
            code_types.append(code_dict[c]['type'])
            code_subtypes.append(code_dict[c]['sub-type'])
        except KeyError as e:
            c = df.loc[i, 'NTEE_CD'][:-1]
            try:
                code_descriptions.append(code_dict[c]['service_summary'])
                code_types.append(code_dict[c]['type'])
                code_subtypes.append(code_dict[c]['sub-type'])
            except KeyError:
                    code_descriptions.append('summary not found.')
                    code_types.append('type not found.')
                    code_subtypes.append('sub-type not found.')
    df['service_summary'] = code_descriptions
    df['service_type'] = code_types
    df['service_subtype'] = code_subtypes
    df['source'] = ['IRS']*len(df)
    print(f'completed compiling dataframe of shape: {df.shape}')
    return df


def locate_potential_duplicate(name, zip, client, collection):
    """Search the desired db collection for services that might be 
       fuzzy dupes of the service you're looking to add.

    Args:
        name (str): name of the service you want to add
        zip (str): string of the zip code of the service you want to add
        client (obj): pymongo MongoClient object
        collection (str): name of the db collection

    Returns:
        str: name of the service that might be a duplicate
    """    
    grammed_name = make_ngrams(name)
    coll = client[dataset][collection]
    dupe_candidate = coll.find_one({"$text": {"$search": ' '.join(grammed_name)}, 'zip': zip})["name"]
    return dupe_candidate


def main(config, client, collection):
    code_dict = config['NTEE_codes']
    df = grab_data(config, code_dict)
    if client[dataset][collection].count() == 0:  # Check if the desired collection is empty
        insert_services(df.to_dict('records'), client, collection)  # No need to check for duplicates in an empty collection
    else:
        refresh_ngrams(client, collection)
        for i in range(len(df)):
            dc = locate_potential_duplicate(df.loc[i, 'NAME'], df.loc[i, 'ZIP'], client, collection)
            if check_similarity(df.loc[i, 'NAME'], dc):
                df.drop(i)
        df = df.reset_index(drop=True)
        insert_services(df.to_dict('records'), client, collection)

if __name__ == "__main__":
    main(config, client, 'tmpIRS')