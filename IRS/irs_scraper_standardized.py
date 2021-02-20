import json
import os
import sys
from datetime import datetime
import re
import requests
import pandas as pd
from tqdm import tqdm
import logging

logger = logging.getLogger(__name__)

_i = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _i not in sys.path:
    # add parent directory to sys.path so utils module is accessible
    sys.path.insert(0, _i)
del _i  # clean up global name space
from shared_code.utils import (
    check_similarity, locate_potential_duplicate,
    insert_services, get_mongo_client
)
from shared_code.base_scraper import BaseScraper

# set the DB user name and password in config
with open('IRS/config.json', 'r') as con:
    # config (dict): config object imported from config.json that will be used to grab data from the CSVs
    config = json.load(con)


class IRSScraper(BaseScraper):
    def grab_data(self):
        """
        Access hosted IRS csv files, append them together,
        clean things up a bit and add code descriptions.

        Returns:
            DataFrame: Pandas DataFrame containing processed data
        """

        # code_dict (dict): dictionary containing NTEE codes and their descriptions
        code_dict = config['NTEE_codes']
        urls = config['dataURLs'].values()
        codes = '|'.join(config['NTEE_codes'].keys())
        df = pd.DataFrame()
        for u in urls:
            logger.info(f'grabbing {u}')
            response = pd.read_csv(u, usecols=self.extract_usecols)
            response.rename(columns=self.rename_columns, inplace=True)
            response.fillna('0', inplace=True)
            logger.info(f'initial shape: {response.shape}')
            response = response[
                response['NTEE_code'].str.contains(codes)
            ]  # filter for desired NTEE codes
            response = response[
                ~response['state'].isin(config['military_mail_locs'])
            ]  # filter out military mail locs
            logger.info(list(set(list(response['state']))))
            response['zip'] = response['zip'].str.slice(start=0, stop=5)  # truncate ZIP codes
            response['NTEE_code'] = response['NTEE_code'].str.slice(start=0, stop=3)  # truncate NTEE codes
            logger.info(f'final shape: {response.shape}')
            df = df.append(response, ignore_index=True)
        df = df.drop_duplicates(
            subset=['name', 'address1', 'city', 'state', 'zip'], ignore_index=True
        )
        code_descriptions = []
        code_types = []
        code_subtypes = []
        for i in tqdm(range(len(df))):
            c = df.loc[i, 'NTEE_code']
            code_descriptions.append(code_dict[c]['service_summary'])
            code_types.append(code_dict[c]['type'])
            code_subtypes.append(code_dict[c]['sub-type'])
        df['description'] = code_descriptions
        df['type'] = code_types
        df['serviceSummary'] = code_subtypes
        df['source'] = ['IRS'] * len(df)
        df.drop(df[df['address1'] == 'NONE'].index, inplace = True)
        logger.info(f'completed compiling dataframe of shape: {df.shape}')
        return df

    def scrape_updated_date(self):
        """
        Check IRS web page with data files to see if the most
        recently updated date is different than the date in MongoDB
        """

        url = self.data_page_url
        resp = requests.get(url).text
        update_statement = re.search(
            r'Updated data posting date: <strong>(\d\d?/\d\d?/\d{4})</strong>', resp
        )
        if update_statement == None:
            return datetime.strptime('1970-01-01', '%Y-%m-%d').date()
        else:
            # scraped_date (datetime): the date that the IRS last updated according to the data-sources collection
            scraped_date = datetime.strptime(
                update_statement.group(1), "%m/%d/%Y"
            ).date()
            return scraped_date


data_source_name = 'irs'

irs_scraper = IRSScraper(
    source=data_source_name,
    data_url='config.json',
    data_page_url='https://www.irs.gov/charities-non-profits/exempt-organizations-business-master-file-extract-eo-bmf',
    data_format="CSV",
    extract_usecols=[
        'EIN', 'NAME', 'STREET', 'CITY', 'STATE', 'ZIP', 'NTEE_CD'
    ],
    drop_duplicates_columns=[
        'NAME', 'STREET', 'CITY', 'STATE', 'ZIP'
    ],
    rename_columns={
        'NAME': 'name', 'STREET': 'address1', 'CITY': 'city', 'STATE': 'state', 'ZIP': 'zip', 'NTEE_CD': 'NTEE_code'
    },
    service_summary='',
    check_collection="services",
    dump_collection="tmpIRSStandardized",
    dupe_collection="tmpIRSStandardizedDuplicates",
    data_source_collection_name=data_source_name,
    collection_dupe_field='name'
)

if __name__ == '__main__':
    client = get_mongo_client()
    irs_scraper.main_scraper(client)
