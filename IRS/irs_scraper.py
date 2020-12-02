import json
import pandas as pd
from pymongo import MongoClient, TEXT
import random
import os
import sys
from tqdm import tqdm
import re
from collections import OrderedDict
import requests
from datetime import datetime, date

if __package__:  # if script is being run as a module
    from ..shelterapputils.utils import check_similarity, refresh_ngrams
else:  # if script is being run as a file
    _i = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _i not in sys.path:
        # add parent directory to sys.path so utils module is accessible
        sys.path.insert(0, _i)
    del _i  # clean up global name space
    from shelterapputils.utils import (
        check_similarity, refresh_ngrams,
        make_ngrams, locate_potential_duplicate,
        distance, insert_services
    )

# set the DB user name and password in config
with open('IRS/config.json', 'r') as con:
    config = json.load(con)

# Establish global variables
client = MongoClient(
    "mongodb+srv://" + os.environ.get('DBUSERNAME') + ":" + os.environ.get('PW')
    + "@shelter-rm3lc.azure.mongodb.net/shelter?retryWrites=true&w=majority"
)['shelter']


def scrape_updated_date():
    url = 'https://www.irs.gov/charities-non-profits/exempt-' \
          'organizations-business-master-file-extract-eo-bmf'
    resp = requests.get(url).text
    update_statement = re.search(
        r'Updated data posting date: (\d\d?/\d\d?/\d{4}) Record', resp
    )
    scraped_date = datetime.strptime(
        update_statement.group(1), "%m/%d/%Y"
    ).date()
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
    return scraped_date > existing_date


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
        ]).rename(columns={
            'NAME': 'name', 'STREET': 'address1',
            'CITY': 'city', 'STATE': 'state', 'ZIP': 'zip'
        }).fillna('0')
        print(f'initial shape: {response.shape}')
        response = response[
            response['NTEE_CD'].str.contains(codes)
        ]  # filter for desired NTEE codes
        response = response[
            ~response['state'].isin(config['military_mail_locs'])
        ]  # filter out military mail locs
        print(list(set(list(response['state']))))
        response['zip'] = response['zip'].str.slice(start=0, stop=5)  # truncate ZIP codes
        print(f'final shape: {response.shape}')
        df = df.append(response, ignore_index=True)
    df = df.drop_duplicates(
        subset=['name', 'address1', 'city', 'state', 'zip'], ignore_index=True
    )
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


def prevent_IRS_EIN_duplicates(EIN, client, collection):
    coll = client[collection]
    dupe = coll.find_one(
        {'EIN': EIN}
    )
    return dupe


def purge_EIN_duplicates(df, client, collection, dupe_collection):
    found_duplicates = []
    for i in range(len(df)):
        EIN = int(df.loc[i, 'EIN'])
        if prevent_IRS_EIN_duplicates(EIN, client, collection):
            found_duplicates.append(i)
    duplicate_df = df.loc[found_duplicates].reset_index(drop=True)
    print('inserting tmpIRS dupes into the dupe collection')
    insert_services(duplicate_df.to_dict('records'), client, dupe_collection)
    df = df.drop(found_duplicates).reset_index(drop=True)
    return df


def main(config, client, check_collection, dump_collection, dupe_collection):
    scraped_update_date = scrape_updated_date()
    try:
        stored_update_date = client['data-sources'].find_one(
            {"name": "irs_exempt_organizations"}
        )['last_updated']
        stored_update_date = datetime.strptime(
            str(stored_update_date), '%Y-%m-%d %H:%M:%S'
        ).date()
        if check_site_for_new_date(stored_update_date):
            print('No new update detected. Exiting script...')
            return
    except KeyError:
        pass
    print('updating scraped update date in data-sources collection')
    client['data_sources'].update_one(
        {"name": "irs_exempt_organizations"},
        {'$set': {'last_updated': str(scraped_update_date)}}
    )
    code_dict = config['NTEE_codes']
    df = grab_data(config, code_dict)
    print('purging EIN duplicates')
    if client[dump_collection].estimated_document_count() > 0:
        df = purge_EIN_duplicates(df, client, dump_collection, dupe_collection)
    if client[check_collection].estimated_document_count() == 0:
        # No need to check for duplicates in an empty collection
        insert_services(df.to_dict('records'), client, dump_collection)
    else:
        print('refreshing ngrams')
        refresh_ngrams(client, check_collection)
        found_duplicates = []
        print('checking for duplicates in the services collection')
        for i in tqdm(range(len(df))):
            dc = locate_potential_duplicate(
                df.loc[i, 'name'], df.loc[i, 'zip'], client, check_collection
            )
            if dc is not False:
                if check_similarity(df.loc[i, 'name'], dc):
                    found_duplicates.append(i)
        duplicate_df = df.loc[found_duplicates].reset_index(drop=True)
        print(f'inserting {duplicate_df.shape[0]} services dupes into the dupe collection')
        if len(duplicate_df) > 0:
            insert_services(duplicate_df.to_dict('records'), client, dupe_collection)
        df = df.drop(found_duplicates).reset_index(drop=True)
        print(f'final df shape: {df.shape}')
        if len(df) > 0:
            insert_services(df.to_dict('records'), client, dump_collection)


if __name__ == "__main__":
    main(config, client, 'services', 'tmpIRS', 'tmpIRSFoundDuplicates')
