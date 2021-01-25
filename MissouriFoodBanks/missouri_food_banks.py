import os
import sys
from datetime import datetime
import re
import numpy as np
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient, errors
from tqdm import tqdm

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

class MFBScraper(BaseScraper):

    '''For this dataset we need to scrape following columns: Name(name),
    location need to be split into (address1, city, state, zip), phone number(phone),
    hoursofoperation(schedule) and add an extra serviceSummary column and default it to "Food Bank" for all entries.'''

    def grab_data(self):
        df = pd.read_csv(self.data_url, usecols=self.extract_usecols)
        df.rename(columns=self.rename_columns, inplace=True)
        df[['address1', 'city', 'lat']] = df.address1.str.split('\n', expand=True)
        df[['city', 'state']] = df.city.str.split(',', expand=True)
        df[['space', 'state', 'zip']] = df.state.str.split(' ', expand=True)
        df['address1'] = np.where(~df['Additional Address Info.'].isnull(),
                                  df['address1'] + ' ' + df['Additional Address Info.'],
                                  df['address1']
                                  )
        df['phone'] = df['phone'].str.replace(r'(\d{3})[-](\d{3}[-]\d{4})', r'(\1) \2')
        df['serviceSummary'] = self.service_summary
        df.drop(['lat', 'space', 'Additional Address Info.'], axis=1, inplace=True)
        return df

data_source_name = "missouri_food_banks"

mfb_scraper = MFBScraper(
    source=data_source_name,
    data_url='https://data.mo.gov/api/views/eb3y-vtsa/rows.csv?accessType=DOWNLOAD',
    data_page_url='https://data.mo.gov/Social-Services/Food-Pantry-List/eb3y-vtsa',
    data_format="CSV",
    extract_usecols=[
        "Agency Name", "Phone Number", "Hours of Operation", "Location", 'Additional Address Info.'
    ],
    drop_duplicates_columns=[
        "Agency Name", "Phone Number", "Hours of Operation", "Location"
    ],
    rename_columns={
        "Agency Name": "name", 'Location': 'address1',
        "Phone Number": "phone", "Hours of Operation": "schedule"
    },
    service_summary="Food Bank",
    check_collection="services",
    dump_collection="tmpMissouriFoodBanks",
    dupe_collection="tmpMissouriFoodBanksDuplicates",
    data_source_collection_name=data_source_name,
    collection_dupe_field='name'
)


if __name__ == '__main__':
    client = get_mongo_client()
    mfb_scraper.main_scraper(client)



