import json
import pandas as pd
from pymongo import MongoClient, TEXT
import random
import os
from tqdm import tqdm
import re
from collections import OrderedDict
import requests
from datetime import datetime, date

# set the DB user name and password in config
with open('IRS/config.json', 'r') as con:
    config = json.load(con)

# Establish global variables
client = MongoClient(
    "mongodb+srv://" + config['userId'] + ":" + config['password']
    + "@shelter-rm3lc.azure.mongodb.net/shelter?retryWrites=true&w=majority"
)['shelter']


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
    return list(OrderedDict.fromkeys(
        name[i:i + size]
        for size in size_range
        for i in range(0, max(0, length - size) + 1)
    ))


def refresh_ngrams(client, collection):
    """Make sure all the services in the desired collection have an ngram field.
       Also ensures that the n-gram field is included
       in the text index for the purpose of searching.

    Args:
        client (obj): pymongo MongoClient object
        collection (str): name of the collection in the db
    """
    coll = client[collection]
    for document in coll.find():
        try:
            coll.update_one(
                {"_id": document["_id"]},
                {"$set": {
                    "ngrams": ' '.join(make_ngrams(document["name"]))
                }
                }
            )
        except KeyError:
            coll.update_one(
                {"_id": document["_id"]},
                {"$set": {
                    "ngrams": ' '.join(make_ngrams(document["NAME"]))
                }
                }
            )
    # Check that ngram field is indexed
    if 'ngrams_text' not in coll.index_information().keys():
        coll.create_index([("ngrams", TEXT)])


def distance(a, b):
    """ Calculates the Levenshtein distance between a and b.
    """
    n, m = len(a), len(b)
    if n > m:
        # Make sure n <= m, to use O(min(n,m)) space
        a, b = b, a
        n, m = m, n

    current = range(n + 1)
    for i in range(1, m + 1):
        previous, current = current, [i] + [0] * n
        for j in range(1, n + 1):
            add, delete = previous[j] + 1, current[j - 1] + 1
            change = previous[j - 1]
            if a[j - 1] != b[i - 1]:
                change = change + 1
            current[j] = min(add, delete, change)

    return 1 - (current[n] / len(a))


def check_similarity(new_service, existing_service):
    regex = r'(st\.? |saint | inc\.?| nfp)'
    new_subbed_service = re.sub(regex, '', new_service)
    existing_subbed_service = re.sub(regex, '', existing_service)
    return distance(new_subbed_service, existing_subbed_service) >= 0.9


def insert_services(data, client, collection):
    """Intake scraped services that have been processed and dupe-checked, and add to MongoDB.

    Args:
        data (dict): dictionary of IRS services containing ID, name, location and NTEE code
        client (obj): MongoClient object
        collection (str): the Mongo collection in which the data should be inserted
    """
    db = client
    db_coll = db[collection]
    db_coll.insert_many(data)


def scrape_updated_date():
    url = 'https://www.irs.gov/charities-non-profits/exempt-' \
          'organizations-business-master-file-extract-eo-bmf'
    resp = requests.get(url).text
    update_statement = re.search(
        r'Updated data posting date: (\d\d?/\d\d?/\d{4}) Record', resp
    )
    scraped_date = datetime.strptime(
        update_statement.group(1), "%m/%d/%Y"
    )
    return scraped_date


def check_site_for_new_date(existing_date):
    """Check IRS web page with data files to see if the most
    recently updated date is different than the date in MongoDB

    Args:
        existing_date (datetime): the date that the IRS last updated,
        according to the data-sources collection

    Returns:
        bool: whether or not the dates are different
    """
    scraped_date = scrape_updated_date()
    if scraped_date > existing_date:
        return True
    else:
        return False


# TODO: convert from print statements to the creation of a log
def grab_data(config, code_dict):
    """Access hosted IRS csv files, append them together,
       clean things up a bit and add code descriptions.

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
        response = response[
            response['NTEE_CD'].str.contains(codes)
        ]  # filter for desired NTEE codes
        response = response[
            ~response['STATE'].isin(config['military_mail_locs'])
        ]  # filter out military mail locs
        print(list(set(list(response['STATE']))))
        response['ZIP'] = response['ZIP'].str.slice(start=0, stop=5)  # truncate ZIP codes
        print(f'final shape: {response.shape}')
        df = df.append(response, ignore_index=True)
    code_descriptions = []
    code_types = []
    code_subtypes = []
    for i in tqdm(range(len(df))):
        try:
            c = df.loc[i, 'NTEE_CD']
            code_descriptions.append(code_dict[c]['service_summary'])
            code_types.append(code_dict[c]['type'])
            code_subtypes.append(code_dict[c]['sub-type'])
        except KeyError:
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
    df['source'] = ['IRS'] * len(df)
    print(f'completed compiling dataframe of shape: {df.shape}')
    return df


def locate_potential_duplicate(name, zipcode, client, collection):
    """Search the desired db collection for services that might be
       fuzzy dupes of the service you're looking to add.

    Args:
        name (str): name of the service you want to add
        zipcode (str): string of the zip code of the service you want to add
        client (obj): pymongo MongoClient object
        collection (str): name of the db collection

    Returns:
        str: name of the service that might be a duplicate
    """
    grammed_name = make_ngrams(name)
    coll = client[collection]
    dupe_candidate = coll.find_one(
        {"$text": {"$search": ' '.join(grammed_name)}, 'ZIP': zipcode}
    )
    if dupe_candidate:
        return dupe_candidate["NAME"]
    return False


def prevent_IRS_EIN_duplicates(EIN, client, collection):
    coll = client[collection]
    dupe = coll.find_one(
        {'EIN': EIN}
    )
    return dupe


def purge_EIN_duplicates(df, client, collection):
    for i in range(len(df)):
        EIN = df.loc[i, 'EIN']
        if prevent_IRS_EIN_duplicates(EIN, client, collection):
            df.drop(i)
    return df.reset_index(drop=True)


def main(config, client, check_collection, dump_collection, dupe_collection):
    stored_update_date = client['data-sources'].find_one(
        {"name": "irs_exempt_organizations"}
    )['last_updated']
    scraped_update_date = scrape_updated_date()
    if check_site_for_new_date(stored_update_date):
        print('No new update detected. Exiting script...')
        return
    client['data_sources'].update_one(
        {"name": "irs_exempt_organizations"},
        {'$set': {'last_updated': scraped_update_date}}
    )
    code_dict = config['NTEE_codes']
    df = grab_data(config, code_dict)
    df = purge_EIN_duplicates(df, client, dump_collection)
    if client[check_collection].count() == 0:  # Check if the desired collection is empty
        # No need to check for duplicates in an empty collection
        insert_services(df.to_dict('records'), client, dump_collection)
    else:
        refresh_ngrams(client, check_collection)
        found_duplicates = []
        for i in range(len(df)):
            dc = locate_potential_duplicate(
                df.loc[i, 'NAME'], df.loc[i, 'ZIP'], client, check_collection
            )
            if check_similarity(df.loc[i, 'NAME'], dc):
                found_duplicates.append(i)
        duplicate_df = df.loc[found_duplicates].reset_index(drop=True)
        insert_services(duplicate_df.to_dict('records'), client, dupe_collection)
        df = df.drop(found_duplicates).reset_index(drop=True)
        insert_services(df.to_dict('records'), client, dump_collection)


if __name__ == "__main__":
    main(config, client, 'services', 'tmpIRS', 'tmpIRSFoundDuplicates')