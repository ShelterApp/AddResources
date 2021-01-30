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

class SummerMealSitesScraper(BaseScraper):
    payload = requests.get('https://opendata.arcgis.com/datasets/9efd2e8ba3104b88921b06fa3f70defb_0.geojson')
    def grab_data(self):
        #Get the data
        data = self.payload.json()

        data = data['features']
        new_data = []
        for item in data:
            if item['properties']:
                new_data.append(item['properties'])
        df = pd.DataFrame(new_data)

        #Extract useful cols
        df = df[['siteName', 'siteStatus','siteAddress','siteCity',
        'siteState','siteZip','sitePhone','Country']]

        #Removing schools, and non-homeless related resources
        ignore_resources_with_keywords = ['school', 'middle', 'elementary', 'high', 'academy','Academy',
         'learn', 'Boy', 'Girl', 'Fire', 'Bus', 'College', 'Apartment', 'Route', 'Magnet']
        filter = df['siteName'].str.contains("|".join(ignore_resources_with_keywords), flags = re.IGNORECASE)
        df = df[~filter]
        df = df.reset_index()
        df = df.drop(['index'], axis = 1)

        #fix address
        df['siteAddress'].str.split(',').str[0]

        #fix phone number
        df['sitePhone'] = df['sitePhone'].apply(lambda x: "({})-{}-{}".format(x[0:3],x[3:6],x[6:10]))

        return super().grab_data(df = df)

    def scrape_updated_date(self):
        data = self.payload.headers
        data = data['x-amz-meta-contentlastmodified']
        return datetime.strptime(data, '%Y-%m-%dT%H:%M:%S.%fZ')

data_url = 'https://opendata.arcgis.com/datasets/9efd2e8ba3104b88921b06fa3f70defb_0.geojson'
scraper = SummerMealSitesScraper(
    source="SummerMealSitesScraper",
    data_url = data_url,
    data_page_url = data_url,
    data_format = "DF",
    extract_usecols=None,
    drop_duplicates_columns=['siteName', 'siteAddress', 'siteZip', 'siteCity', 'siteState'],
    rename_columns={'siteName':'name', 'siteStatus':'notes','siteAddress':'address1','siteCity':'city',
    'siteState':'state','siteZip':'zip','sitePhone':'phone','Country':'country'
    },
    service_summary="Food Bank",
    check_collection="services",
    dump_collection="tmp_summer_meal_sites_dump",
    dupe_collection="tmp_summer_meal_sites_dupe",
    data_source_collection_name="SummerMealSitesScraper",
    collection_dupe_field='name'
    )


if __name__ == '__main__':
    client=  get_mongo_client()
    scraper.main_scraper(client)
