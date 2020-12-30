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
    insert_services, client
)
from shared_code.base_scraper import BaseScraper


class LHBScraper(BaseScraper):

    def scrape_updated_date(self):

        resp = super().scrape_updated_date()
        soup = BeautifulSoup(resp, 'html.parser')
        date_string = soup.find('relative-time', class_='no-wrap')
        date_string = date_string.attrs['datetime']
        date_string = re.search(
            r'(19|20)\d\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])',
            date_string
        ).group(0)
        scraped_update_date = datetime.strptime(
            date_string, '%Y-%m-%d'
        )
        return scraped_update_date.date()


       def grab_data(self):

        '''Subcategory would be serviceSummary, Servicename would be name, PhoneNumber would be phone,
        Physical Address should be split into address1, city, state, zip, Hours of operation is schedule,
         Description can be description, Web Address would be website, Email Address would be contact Email'''


        df = pd.read_csv(self.data_url, usecols=self.extract_usecols)
        df.drop_duplicates(
            subset=self.drop_duplicates_columns,
            inplace=True,
            ignore_index=True
        )
        df.rename(columns=self.rename_columns, inplace=True)
        df['phone'] = df['phone'].str.replace(r'(\d{3})\s*[-.]\s*(\d{3}\s*[-.]\s*\d{4})', r'(\1) \2')
        df.replace('', np.nan, inplace=True)
        df.dropna(subset=['name'], inplace=True)
        df[['address1', 'state', 'zip']] = df['Physical Address'].str.extract(r'(.+)(OR).+(\d{5})', expand=True)
        df['serviceSummary'] = np.where(df['serviceSummary'].isnull(), 
                                        df['Category'], 
                                        df['serviceSummary'])
        df['source'] = self.source
        df.drop(['Physical Address', 'Category'], axis=1, inplace=True)
        return df


lhb_scraper = LHBScraper(
    source="LittleHelpBook",
    data_url='https://github.com/OpenEugene/little-help-book-data/raw/master/data/little-help-book.csv',
    data_page_url='https://github.com/OpenEugene/little-help-book-data/blob/master/data/little-help-book.csv',
    data_format="CSV",
    extract_usecols=[
        "Subcategory", "Service Name", "Phone Number", 'Hours of operation',
        'Physical Address', 'Description', 'Web address', 'Email Address'
    ],
    drop_duplicates_columns=[
        "Subcategory", "Service Name", "Phone Number", 'Hours of operation',
        'Physical Address', 'Description', 'Web address', 'Email Address'
    ],
    rename_columns={
        "Subcategory": "serviceSummary", "Service Name": "name", "Phone Number": 'phone',
        'Hours of operation': 'schedule', 'Description': 'description', 'Web address': 'website',
        'Email Address': 'contactEmail'
    },
    service_summary="",
    check_collection="services",
    dump_collection="tmpLittleHelpBook",
    dupe_collection="tmpLHBDuplicates",
    data_source_collection_name="LittleHelpBook",
    collection_dupe_field='name'
)

if __name__ == "__main__":
    scraped_update_date = lhb_scraper.scrape_updated_date()
    stored_update_date = lhb_scraper.retrieve_last_scraped_date(client)
    if stored_update_date is not None:
        if scraped_update_date < stored_update_date:
            logging.info('No new data. Goodbye...')
            sys.exit()
    lhb_scraper.main_scraper(client)
    
    
