import json
import pandas as pd
from pymongo import MongoClient
import random
import os
from tqdm import tqdm

# set the DB user name and password in config
with open('config.json', 'r') as con:
  config = json.load(con)

client = MongoClient(
    "mongodb+srv://" 
    + config['userId'] + ":" 
    + config['password'] 
    + "@shelter-rm3lc.azure.mongodb.net/shelter?retryWrites=true&w=majority"
)
db = client['shelter']
db_coll = db['services']


def insert_services(data, client, collection):
    """Intake scraped services that have been processed and dupe-checked, and add to MongoDB.

    Args:
        data (dict): dictionary of IRS services containing ID, name, location and NTEE code
        client (obj): MongoClient object
        collection (str): the Mongo collection in which the data should be inserted
    """    
    db = client['shelter']
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
    return df

ntee_codes = pd.read_csv('ntee_codes.csv')
ntee_codes = ntee_codes[ntee_codes['NTEE Code'].isin(config['NTEE_codes'].keys())].reset_index(drop=True)
code_dict = {
    ntee_codes.loc[i,'NTEE Code']: ntee_codes.loc[i, 'Description'] for i in range(len(ntee_codes))
}

if __name__ == "__main__":
    print(code_dict)
    df = grab_data(config, config['NTEE_codes'])
    df.to_csv('df.csv', index=False)
    #todo: check df for duplicates
    # insert_services(df.to_dict('records'), client, 'tmpIRS')  FOR ONCE DUPE-CHECKING IS COMPLETED
