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


class NYC_Scraper(BaseScraper):

    def grab_data(self):

        df = pd.read_csv(self.data_url, usecols=self.extract_usecols)
        df.drop_duplicates(subset=self.drop_duplicates_columns,
                           inplace=True,
                           ignore_index=True
                           )
        df.rename(columns=self.rename_columns, inplace=True)
        df[['address1']] = df['address1'].str.replace('\n', ' ')
        df[['address1']] = df['address1'].str.extract('(.+)(?=NY)')
        df['state'], df['city'], df['serviceSummary'], df['source'] = 'NY', 'New York', self.service_summary, self.source
        return df


nyc_scraper = NYC_Scraper(
    source = "NYCDropInCenters",
    data_url = 'https://data.cityofnewyork.us/api/views/bmxf-3rd4/rows.csv?accessType=DOWNLOAD',
    data_page_url = 'https://data.cityofnewyork.us/Social-Services/Directory-Of-Homeless-Drop-In-Centers/bmxf-3rd4',
    data_format = "CSV",
    extract_usecols = [
                          "Center Name", "Address", "Postcode"
                      ],
    drop_duplicates_columns = [
                                  "Center Name", "Address", "Postcode"
                              ],
    rename_columns = {
                         "Center Name": "name", "Address": 'address1', 'Postcode': 'zip'
                     },
    service_summary = "Drop in centers",
    check_collection = "services",
    dump_collection = "tmpNYCDropInCenters",
    dupe_collection = "tmpNYCDropInCentersFoundDuplicates",
    data_source_collection_name = "nyc_drop_in_centers",
    collection_dupe_field = 'name'
    )

if __name__ == '__main__':
    nyc_scraper.main_scraper(client)

    
    
