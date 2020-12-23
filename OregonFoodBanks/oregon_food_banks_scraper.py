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


class OFB_Scraper(BaseScraper):

    ''' we need to pull only address, city, state, zip, name,
    serviceSummary we use food bank and registry as registryID'''


    def grab_data(self):

        df = pd.read_csv(self.data_url, usecols=self.extract_usecols)
        df.drop_duplicates(
            subset=self.drop_duplicates_columns,
            inplace=True,
            ignore_index=True
        )
        df.rename(columns=self.rename_columns, inplace=True)
        df['address1'] = np.where(~df['address2'].isnull(),
                                  df['address1'] + ' ' + df['address2'],
                                  df['address1']
                                  )
        df['serviceSummary'] = self.service_summary
        df['state'] = 'OR'
        df[['zip']] = [str(int(x)) for x in df['zip']]
        df[['registryID']] = [str(int(x)) for x in df['registryID']]
        df.drop(['address2'], axis=1, inplace=True)
        return df



ofb_scraper = OFB_Scraper(
    source="OregonFoodBanks",
    data_url='https://data.oregon.gov/api/views/nvp3-5wtz/rows.csv?accessType=DOWNLOAD',
    data_page_url='https://data.oregon.gov/Business/Filtered-Businesses-Food-Banks/nvp3-5wtz',
    data_format="CSV",
    extract_usecols=[
        "Registry Number", "Business Name", "Address", "Address Continued", 'City', 'Zip'
    ],
    drop_duplicates_columns=[
        "Registry Number", "Business Name", "Address", "Address Continued", 'City', 'Zip'
    ],
    rename_columns={
        "Registry Number": "registryID", "Address": 'address1',
        "Address Continued": "address2", 'City': "city", 'Zip':'zip', "Business Name":'name'
    },
    service_summary="Food Bank",
    check_collection="services",
    dump_collection="tmpOregonFoodBanks",
    dupe_collection="tmpOregonFoodBanksFoundDuplicates",
    data_source_collection_name="oregon_food_banks",
    collection_dupe_field='name',
)

if __name__ == '__main__':
    ofb_scraper.main_scraper(client)