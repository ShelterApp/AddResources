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

class MFB_Scraper(BaseScraper):

    '''For this dataset we need to scrape following columns: FACILITY_NAME(name), PROVIDER(provider), CITY(city),
    ZIP(zip), WARD(ward), TYPE(type), SUBTYPE(subtype), URL(url), ON_SITE_MEDICAL_CLINIC(medical_clinic),
    AGES_SERVED(ages_served), HOW_TO_ACCESS(how_to_access),
    and add an extra serviceSummary column and default it to "Emergency Shelter" for all entries,
    and add an extra state column and default it to "DC" for all entries.'''

    def grab_data(self):
        df = pd.read_csv(self.data_url, usecols=self.extract_usecols)
        df.rename(columns=self.rename_columns, inplace=True)
        df['medical_clinic'] = np.where(~df['medical_clinic'].isnull(), df['medical_clinic'], 'No')
        df['serviceSummary'] = self.service_summary
        df['state'] = 'DC'
        return df


mfb_scraper = MFB_Scraper(
    source="WashingtonDCServices",
    data_url='https://opendata.arcgis.com/datasets/87c5e68942304363a4578b30853f385d_25.csv',
    data_page_url='https://opendata.dc.gov/datasets/87c5e68942304363a4578b30853f385d_25/data',
    data_format="CSV",
    extract_usecols=[
        "FACILITY_NAME", "PROVIDER", "ADDRESS", "CITY", "ZIP", "WARD", "TYPE", "SUBTYPE", "URL",
        "ON_SITE_MEDICAL_CLINIC", "AGES_SERVED", "HOW_TO_ACCESS"
    ],
    drop_duplicates_columns=[
        "FACILITY_NAME", "PROVIDER", "ADDRESS", "CITY", "ZIP", "WARD", "TYPE", "SUBTYPE", "URL",
        "ON_SITE_MEDICAL_CLINIC", "AGES_SERVED", "HOW_TO_ACCESS"
    ],
    rename_columns={
        "FACILITY_NAME": "name", "PROVIDER": "provider", 'ADDRESS': 'address1', "CITY": 'city', 'ZIP': 'zip',
        "WARD": "ward", "TYPE": "service_type", "SUBTYPE": "people_served", 'URL': 'website',
        "ON_SITE_MEDICAL_CLINIC": "medical_clinic", "AGES_SERVED": "ages_served", "HOW_TO_ACCESS": "how_to_access"
    },
    service_summary="Emergency Shelter",
    check_collection="services",
    dump_collection="tmpWashingtonDCShelters",
    dupe_collection="tmpWashingtonDCSheltersFoundDuplicates",
    data_source_collection_name="Washington_DC_Shelters",
    collection_dupe_field='name'
)


if __name__ == '__main__':
    mfb_scraper.main_scraper(client)
