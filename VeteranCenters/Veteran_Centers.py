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
from io import StringIO

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

class VeteranCentersScraper(BaseScraper):

    def grab_data(self):
        url = 'https://sandbox-api.va.gov/services/va_facilities/v0/facilities/all'
        headers = {'apikey': '1oavZAY4gJeEPE6atbHdXNN8hvKrXXYX', 'accept': 'text/csv'}
        response = requests.get(url, headers=headers)
        response = StringIO(response.text)
        df = pd.read_csv(response, usecols=self.extract_usecols)
        df.drop_duplicates(
            subset=self.drop_duplicates_columns,
            inplace=True,
            ignore_index=True
        )
        df = df.fillna("")
        df = df.astype(str)
        df['address1'] = df['physical_address_1'] + ' ' + df['physical_address_2'] + ' ' + df['physical_address_3']
        df['serviceSummary'] = df['facility_type'] + ' ' + df['classification']
        df['phone'] = df['phone_main'].str.replace(r'(\d{3})\s*[-.]\s*(\d{3}\s*[-.]\s*\d{4})', r'(\1) \2')
        df['zip'] = df['physical_zip'].apply(
            lambda z: z[0:5] if "-" in z else z
        )
        df.drop(['phone_main', 'facility_type', 'physical_zip', 'physical_address_3', 'physical_address_2',
                 'physical_address_1', 'classification'], axis=1, inplace=True)
        return df



data_source_name = 'veteran_shelters'

veteran_shelters_scraper = VeteranCentersScraper(
    source=data_source_name,
    data_url='https://sandbox-api.va.gov/services/va_facilities/v0/facilities/all',
    data_page_url='',
    data_format="CSV",
    extract_usecols=[
        "name", "facility_type", "website", "phone_main", "physical_city", "physical_state",
        "physical_zip", "physical_address_3", "physical_address_2", "physical_address_1", "classification"
    ],
    drop_duplicates_columns=[
        "name", "website", "phone_main", "physical_address_3", "physical_address_2", "physical_address_1"
    ],
    rename_columns={
        "physical_city": 'city', 'physical_state': 'state'
    },
    service_summary="",
    check_collection="services",
    dump_collection="tmpVeteranCenters",
    dupe_collection="tmpVeteranCentersDuplicates",
    data_source_collection_name=data_source_name,
    collection_dupe_field='name',
)

if __name__ == '__main__':
    veteran_shelters_scraper.main_scraper(client)