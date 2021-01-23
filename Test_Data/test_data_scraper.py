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
    insert_services, get_mongo_client
)
from shared_code.base_scraper import BaseScraper

#This scrapper tests if the script validator can remove any rows with invalid data
class TestDataScraper(BaseScraper):
    def grab_data(self):
        df = pd.read_csv(self.data_url, usecols=self.extract_usecols)
        df.rename(columns=self.rename_columns, inplace=True)
        return df

data_source_name = 'test data scraper'

test_data_scraper = TestDataScraper(
    source=data_source_name,
    data_url='./Test_Data/test_data.csv',
    data_page_url='https://www.google.com/',
    data_format="CSV",
    extract_usecols=[
        "Name", "Phone", "Address", "Mail", "City", "State", "Zip", "Website", "Summary"
    ],
    drop_duplicates_columns=[
        "Name", "Phone"
    ],
    rename_columns={
        "Name": "name", 'Address': 'address1', 'Mail': 'address2', "Phone": "phone", "City": 'city', 'Zip': 'zip',
        'Website': 'website', 'State': 'state', "Summary": 'serviceSummary'
    },
    service_summary='Food Bank',
    check_collection="services",
    dump_collection="tmpTestData",
    dupe_collection="tmpTestDataDuplicates",
    data_source_collection_name=data_source_name,
    collection_dupe_field='name'
)

if __name__ == '__main__':
    client = get_mongo_client()
    test_data_scraper.main_scraper(client)



