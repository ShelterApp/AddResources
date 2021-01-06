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
import logging

logger = logging.getLogger(__name__)

_i = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _i not in sys.path:
    # add parent directory to sys.path so utils module is accessible
    sys.path.insert(0, _i)
del _i  # clean up global name space
from shared_code.utils import (
    check_similarity, locate_potential_duplicate,
    insert_services, client
)
from shared_code.base_scraper import BaseScraper

class BCFoodBanksScraper(BaseScraper):

    '''For this dataset we need to scrape following columns: FCLTY_NM(name),
    ST_ADDRESS(address1), LOCALITY(city), POSTAL_CD(zip), CONT_PHONE(phone),
    and add an extra serviceSummary column and default it to "Food Bank" for all entries,
    and add an extra state column and default it to "BC" for all entries.'''

    def grab_data(self):
        path = 'https://drive.google.com/uc?export=download&id=' + self.data_url.split('/')[-2]
        df = pd.read_csv(path, usecols=self.extract_usecols)
        df.rename(columns=self.rename_columns, inplace=True)
        df['phone'] = df['phone'].str.replace(r'(\d{3})[-](\d{3}[-]\d{4})', r'(\1) \2')
        df['address1'] = np.where(~df['address1'].isnull(),
                                  df['address1'],
                                  df['MAIL_ADD'] #we want to use MAIL_ADD when ST_ADDRESS column is null, drop the column after
                                  )
        df['website'] = np.where(~df['website'].isnull(),
                                  df['website'],
                                  ''
                                  )
        df['serviceSummary'] = self.service_summary
        df['state'] = 'BC'
        #dropping the column because we used it's value for address1 when ST_ADDRESS was null.
        df.drop(columns=['MAIL_ADD'], inplace=True)
        return df

    def scrape_updated_date(self) -> str:
        resp = super().scrape_updated_date()
        soup = BeautifulSoup(resp, 'html.parser')
        table_rows = soup.find_all('tr')
        found = False
        for r in table_rows:
            for i, d in enumerate(r.find_all('td')):
                if i == 0: 
                    if d.text.strip() == 'Record Last Modified':
                        found = True
                if i == 1 and found:
                    data_source_modified_on = d.text.strip()
                    break
            if found:
                break
        if found:
            scraped_update_date = datetime.strptime(data_source_modified_on, '%Y-%m-%d')
            return scraped_update_date.date()
        else: 
            return datetime.strptime('1970-01-01T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ').date()

data_source_name = 'bc_food_banks'

bc_food_banks_scraper = BCFoodBanksScraper(
    source=data_source_name,
    #todo: Change this data_url from a single file to lastest file in corresonding google drive folder.
    #because there is not an easy way to download this data programmatically.
    data_url='https://drive.google.com/file/d/1a5GEFXqmlKWM01FenakScIMknU1PMICq/view?usp=sharing',
    data_page_url='https://catalogue.data.gov.bc.ca/dataset/food-banks', #need to change the URL
    data_format="CSV",
    extract_usecols=[
        "FCLTY_NM", "CONT_PHONE", "ST_ADDRESS", "MAIL_ADD", "LOCALITY", "POSTAL_CD", "WEBSITE"
    ],
    drop_duplicates_columns=[
        "FCLTY_NM", "CONT_PHONE", "LOCALITY", "POSTAL_CD"
    ],
    rename_columns={
        "FCLTY_NM": "name", 'ST_ADDRESS': 'address1',
        "CONT_PHONE": "phone", "LOCALITY": 'city', 'POSTAL_CD': 'zip', 'WEBSITE': 'website'
    },
    service_summary="Food Bank",
    check_collection="services",
    dump_collection="tmpBCFoodBanks",
    dupe_collection="tmpBCFoodBanksDuplicates",
    data_source_collection_name=data_source_name,
    collection_dupe_field='name'
)

if __name__ == '__main__':
    bc_food_banks_scraper.main_scraper(client)



