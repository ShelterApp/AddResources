import os
import sys
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient, errors
from tqdm import tqdm

if __package__:  # if script is being run as a module
    from ..shelterapputils.utils import (
        check_similarity, locate_potential_duplicate,
        insert_services, client
    )
    from ..shelterapputils.base_scraper import BaseScraper
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
    from shelterapputils.base_scraper import BaseScraper


class BHS_Scraper(BaseScraper):

    '''def scrape_updated_date(data_page_url):
        resp = requests.get(data_page_url, timeout=(6.05, 15)).text
        soup = BeautifulSoup(resp, 'html.parser')
        date_string = soup.find('span', class_='date').get_text
        print(date_string)'''

    def grab_data(self):
        df = pd.read_csv(self.data_url, usecols=self.extract_usecols)
        df = df.dropna(subset=['zipCode', 'Location 1']).reset_index(drop=True)
        df.rename(columns=self.rename_columns, inplace=True)
        df[['address', 'state']] = df.address.str.split('\n', expand=True)
        df[['state']] = df['state'].str[-2:]
        df[['zip']] = [str(int(x)) for x in df['zip']]
        return df


bhs_scraper = BHS_Scraper(
    source="BaltimoreHomelessShelters",
    data_url="https://data.baltimorecity.gov/api/views/hyq3-8sxr/rows.csv?accessType=DOWNLOAD",
    data_page_url='https://data.baltimorecity.gov/Health/Homeless-Shelters/hyq3-8sxr',
    data_format="CSV",
    extract_usecols=[
        "name", "type", "zipCode", "Location 1"
    ],
    drop_duplicates_columns=[
        "name", "type", "zipCode", "Location 1"
    ],
    rename_columns={
        "Location 1": "address",
        "zipCode": "zip", "type": "serviceSummary"
    },
    service_summary="Homeless Shelter",
    check_collection="services",
    dump_collection="tmpBaltimoreHomelessShelters",
    dupe_collection="tmpBaltimoreHomelessSheltersFoundDuplicates",
    data_source_collection_name="baltimore_homeless_shelters",
    collection_dupe_field='name'
)


if __name__ == '__main__':
    bhs_scraper.main_scraper(client)
