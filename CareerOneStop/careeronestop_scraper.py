import os
import sys
from datetime import datetime

import pandas as pd
from pymongo import MongoClient, errors
from tqdm import tqdm

if __package__:  # if script is being run as a module
    from ..shelterapputils.utils import (
        check_similarity, locate_potential_duplicate,
        insert_services, client
    )
else:  # if script is being run as a file
    _i = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if _i not in sys.path:
        # add parent directory to sys.path so utils module is accessible
        sys.path.insert(0, _i)
    del _i  # clean up global name space
    from shelterapputils.utils import (
        check_similarity, locate_potential_duplicate,
        insert_services, client
    )


def grab_data():
    """Access hosted HUD csv file,
       clean things up a bit and add code descriptions.

    Returns:
        DataFrame: Pandas DataFrame containing processed data
    """
    data_url = 'https://www.careeronestop.org/TridionMultimedia/tcm24-49673_XLS_AJC_Data_11172020.xls'
    df = pd.read_excel(
        data_url,
        usecols=[
            'Name of Center', 'Address1', 'Address2',
            'City', 'State', 'Zip Code',
            'Phone', 'Email Address', 'Web Site URL', 'Office Hours',
        ]
    )
    df.drop_duplicates(
        subset=['Name of Center', 'Address1', 'Address2', 'City', 'State', 'Zip Code'],
        inplace=True,
        ignore_index=True
    )
    df.rename(columns={
        'Name of Center': "name", 'Address1': "address1", 'Address2': "address2",
        'City': "city", 'State': 'state', 'Zip Code': "zip", 'Phone': "phone",
        'Email Address': "email", 'Web Site URL': "url", 'Office Hours': "hours"},
        inplace=True)
    df.applymap(lambda x: x if not x or not isinstance(x, str) else x.strip())
    df['zip'] = df['zip'].apply(lambda z: z[0:5] if "-" in z else z)
    df['source'] = ['CareerOneStop'] * len(df)
    df['service_summary'] = 'Employment Assistance'
    return df


def main(client: MongoClient, check_collection: str, dump_collection: str, dupe_collection: str):
    """

    :param client:              MongoDB Client for MongoDB operations.
    :param check_collection:    This is the master collection that new scraped data will be merged into.
                                It is also the collection that will be checked for duplicates.
    :param dump_collection:     This is the temporary holding collection for freshly scraped data.
    :param dupe_collection:     This is the temporary collection that duplicates are put into.
    :return:
    """
    df = grab_data()
    print('purging duplicates from existing CareerOneStop collection')
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
        print('updating scraped update date in data-sources collection')
        try:
            client['data_sources'].update_one(
                {"name": "irs_exempt_organizations"},
                {'$set': {'last_updated': datetime.strftime(datetime.now(), '%m/%d/%Y')}}
            )
        except errors.OperationFailure as e:
            print(e)


if __name__ == "__main__":
    main(client, 'services', 'tmpCareerOneStop', 'tmpCareerOneStopFoundDuplicates')
