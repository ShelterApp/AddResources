import os
import sys
import json
import pandas as pd
from pymongo import MongoClient, TEXT, errors
import random
from tqdm import tqdm
import re
from collections import OrderedDict
import requests
from datetime import datetime, date

if __package__:  # if script is being run as a module
    from ..shelterapputils.utils import (
        check_similarity, refresh_ngrams,
        make_ngrams, locate_potential_duplicate,
        distance, insert_services, client
    )
else:  # if script is being run as a file
    _i = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _i not in sys.path:
        # add parent directory to sys.path so utils module is accessible
        sys.path.insert(0, _i)
    del _i  # clean up global name space
    from shelterapputils.utils import (
        check_similarity, refresh_ngrams,
        make_ngrams, locate_potential_duplicate,
        distance, insert_services, client
    )


def grab_data():
    """Access hosted HUD csv file,
       clean things up a bit and add code descriptions.

    Returns:
        DataFrame: Pandas DataFrame containing processed data
    """
    data_url = 'https://www.huduser.gov/portal/sites/default/files/xls/2019' \
        '-Housing-Inventory-County-RawFile.xlsx'
    df = pd.read_excel(
        data_url,
        usecols=[
            'Row #', 'CocState', 'Geo Code', 'CoC', 'Coc\ID', 'HudNum', 'Organization ID',
            'Organization Name', 'HMIS Org ID', 'Project ID', 'Target Population',
            'Project Name', 'HMIS Project ID', 'HIC Date', 'Project Type',
            'Bed Type', 'Victim Service Provider', 'address1',
            'address2', 'city', 'state', 'zip', 'Total Beds', 'Updated On'
        ]
    )
    df = df.drop_duplicates(
        subset=[
            'Organization Name', 'Project Name', 'address1', 'city', 'state', 'zip'
        ],
        ignore_index=True
    ).reset_index(drop=True)
    df['source'] = ['HUD'] * len(df)
    df['service_type'] = ['SHELTER'] * len(df)
    service_summaries = []
    for i in tqdm(range(len(df))):
        if df.loc[i, 'Project Type'] == 'ES':
            if df.loc[i, 'Target Population'] is not None:
                service_summaries.append('Domestic Violence Shelter')
            else:
                service_summaries.append('Emergency Shelter')
        else:
            service_summaries.append('Transitional Housing')
    df['service_summary'] = service_summaries
    return df


def scrape_updated_date():
    url = 'https://www.hudexchange.info/resource/3031/pit-and-hic-data-since-2007/'
    resp = requests.get(url).text
    update_statement = re.search(
        r'Date Published: (\w{3,8} \d{4})', resp
    )
    scraped_date = datetime.strptime(
        update_statement.group(1), "%B %Y"
    ).date()
    return scraped_date


def check_site_for_new_date(existing_date):
    """Check HUD web page with data files to see if the most
    recently updated date is different than the date in MongoDB

    Args:
        existing_date (datetime): the date that the IRS last updated,
        according to the data-sources collection

    Returns:
        bool: whether or not the dates are different
    """
    scraped_date = scrape_updated_date()
    return scraped_date > existing_date


def prevent_HUD_duplicates(pID, client, collection):
    coll = client[collection]
    dupe = coll.find_one(
        {'Project ID': pID}
    )
    return dupe


def purge_HUD_duplicates(df, client, collection, dupe_collection):
    found_duplicates = []
    for i in range(len(df)):
        pID = int(df.loc[i, 'Project ID'])
        if prevent_HUD_duplicates(pID, client, collection):
            found_duplicates.append(i)
    duplicate_df = df.loc[found_duplicates].reset_index(drop=True)
    print('inserting tmpHUD dupes into the dupe collection')
    insert_services(duplicate_df.to_dict('records'), client, dupe_collection)
    df = df.drop(found_duplicates).reset_index(drop=True)
    return df


def main(client, check_collection, dump_collection, dupe_collection):
    scraped_update_date = scrape_updated_date()
    print(str(scraped_update_date))
    try:
        stored_update_date = client['data-sources'].find_one(
            {"name": "hud_pit_hic_data"}
        )['last_updated']
        stored_update_date = datetime.strptime(
            str(stored_update_date), '%Y-%m-%d %H:%M:%S'
        ).date()
        print(stored_update_date)
        print(check_site_for_new_date(stored_update_date))
        if not check_site_for_new_date(stored_update_date):
            print('No new update detected. Exiting script...')
            return
    except KeyError:
        print('Key Error')
        pass
    df = grab_data()
    print('purging duplicates from existing HUD collection')
    if client[dump_collection].estimated_document_count() > 0:
        df = df  # purge_HUD_duplicates(df, client, dump_collection, dupe_collection)
    if client[check_collection].estimated_document_count() == 0:
        # No need to check for duplicates in an empty collection
        insert_services(df.to_dict('records'), client, dump_collection)
    else:
        print('refreshing ngrams')
        # refresh_ngrams(client, check_collection)
        found_duplicates = []
        print('checking for duplicates in the services collection')
        for i in tqdm(range(len(df))):
            dc = locate_potential_duplicate(
                df.loc[i, 'Project Name'], df.loc[i, 'zip'], client, check_collection
            )
            if dc is not False:
                if check_similarity(df.loc[i, 'Project Name'], dc):
                    found_duplicates.append(i)
        duplicate_df = df.loc[found_duplicates].reset_index(drop=True)
        print(f'inserting {duplicate_df.shape[0]} services dupes into the dupe collection')
        if len(duplicate_df) > 0:
            insert_services(duplicate_df.to_dict('records'), client, dupe_collection)
        df = df.drop(found_duplicates).reset_index(drop=True)
        print(f'final df shape: {df.shape}')
        if len(df) > 0:
            insert_services(df.to_dict('records'), client, dump_collection)
        print('updating scraped update date in data-sources collection')
        try:
            client['data_sources'].update_one(
                {"name": "irs_exempt_organizations"},
                {'$set': {'last_updated': datetime.strftime(scraped_update_date, '%m/%d/%Y')}}
            )
        except errors.OperationFailure as e:
            print(e)


if __name__ == "__main__":
    main(client, 'services', 'tmpHUD', 'tmpHUDFoundDuplicates')
