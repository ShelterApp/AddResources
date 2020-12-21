import os
import sys
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
from pymongo import MongoClient, errors
from tqdm import tqdm

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


class NWH_Scraper(BaseScraper):
    def grab_data(self) -> pd.DataFrame:
        ak = os.environ.get('NWH_API_KEY')
        headers = {'Authorization': f'Bearer {ak}'}
        params = {'fields': self.extract_usecols}
        print('grabbing the first 100')
        resp = requests.get(self.data_url, params=params, headers=headers).json()
        df = pd.DataFrame([r['fields'] for r in resp['records']])
        offset = resp['offset']
        while offset is not None:
            params['offset'] = offset
            print(f'grabbing another 100. Offset: {offset}')
            next_output = requests.get(self.data_url, params=params, headers=headers).json()
            ndf = pd.DataFrame([r['fields'] for r in next_output['records']])
            df = df.append(ndf)
            if 'offset' in next_output:
                offset = next_output['offset']
            else:
                offset = None
        print(df.columns)
        df = super().grab_data(df=df)
        df['full address'].replace('', np.nan, inplace=True)
        df = df.dropna(subset=['full address']).reset_index(drop=True)
        df['address'] = df['full address'].apply(lambda x: re.search("(.*), \n", x).group(1))
        cities = []
        for i in range(len(df)):
            try:
                cities.append(re.search(", \n(.*), WA", df.loc[i, 'full address']).group(1))
            except AttributeError:
                cities.append(np.nan)
        df['city'] = cities
        df['state'] = ['WA'] * len(df)
        df['zip'] = df['full address'].str[-5:]
        df = df.replace('\n', '', regex=True)
        names = []
        for i in range(len(df)):
            try:
                names.append(re.search(r"^(.*) - \(\d", df.loc[i, 'summary']).group(1))
            except Exception:
                names.append(df.loc[i, 'summary'].replace(r' - ?$', ''))
        df['name'] = names
        df.applymap(lambda x: x if not x or not isinstance(x, str) else x.strip())
        return df


nwh_scraper = NWH_Scraper(
    source="NW Hospitality",
    data_url='https://api.airtable.com/v0/appg1LlpHfy8ckmRu/Homeless%20Resources'
    '%20in%20Washington%20State%20-%20Curated%20by%20Northwest%20Hospitality',
    data_page_url='https://api.airtable.com/v0/appg1LlpHfy8ckmRu/Homeless%20Resources'
    '%20in%20Washington%20State%20-%20Curated%20by%20Northwest%20Hospitality',
    data_format="DF",
    extract_usecols=[
        'Summary', 'Full Address', 'Category',
        'Email', 'Website'
    ],
    drop_duplicates_columns=[
        'Summary', 'Full Address',
        'Email', 'Website'
    ],
    rename_columns={
        "Summary": "summary", "Full Address": "full address",
        'Email': 'email', "Website": "url", "Category": "service_summary"
    },
    service_summary="service_summary",
    check_collection="services",
    dump_collection="tmpNWHospitality",
    dupe_collection="tmpNWHospitalityFoundDuplicates",
    data_source_collection_name="nw_hospitality",
    collection_dupe_field='summary'
)


if __name__ == "__main__":
    nwh_scraper.main_scraper(client)
