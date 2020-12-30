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


class NYC_Youth_Scraper(BaseScraper):


    def grab_data(self) -> pd.DataFrame:

        df = super().grab_data()
        df['state'], df['city'] = 'NY', 'New York'
        df['phone'] = df['phone'].str.replace(r'(\d{3})[.](\d{3})[.](\d{4})', r'(\1) \2-\3')
        return df


nyc_youth_scraper = NYC_Youth_Scraper(
    source = "NYCRunawayAndHomelessYouth",
    data_url = 'https://data.cityofnewyork.us/api/views/ujsc-un6m/rows.csv?accessType=DOWNLOAD',
    data_page_url = 'https://data.cityofnewyork.us/Social-Services/DYCD-after-school-programs-Runaway-And-Homeless-Yo/ujsc-un6m',
    data_format = "CSV",
    extract_usecols = [
                          "PROGRAM", "SITE NAME", "Contact Number",
        'Number and Street Address', 'Postcode'
                      ],
    drop_duplicates_columns = [
                                  "PROGRAM", "SITE NAME", "Contact Number",
        'Number and Street Address', 'Postcode'
                              ],
    rename_columns = {
                         "SITE NAME": "name", "Number and Street Address": 'address1', 'Postcode': 'zip',
    "PROGRAM":'serviceSummary', 'Contact Number':'phone'
                     },
    service_summary = [''],
    check_collection = "services",
    dump_collection = "tmpNYCRunawayAndHomelessYouth",
    dupe_collection = "tmpNYCRunawayAndHomelessYouthFoundDuplicates",
    data_source_collection_name = "nyc_runaway_and_homeless_youth",
    collection_dupe_field = 'name'
    )


if __name__ == '__main__':
    nyc_youth_scraper.main_scraper(client)
