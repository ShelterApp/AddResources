import os
import sys
import logging

from datetime import datetime
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


class COS_Scraper(BaseScraper):
    def scrape_updated_date(self) -> str:
        resp = super().scrape_updated_date()
        soup = BeautifulSoup(resp, 'html.parser')
        table_rows = soup.find_all('tr')
        found = False
        for r in table_rows:
            for i, d in enumerate(r.find_all('td')):
                if i == 0:
                    if d.text.strip() == 'Comprehensive and Affiliate American Job Centers':
                        found = True
                if i == 1 and found is True:
                    month_string = d.text                    
                    break
            if found:
                break
        scraped_update_date = datetime.strptime(
            month_string, '%B %Y'
        )
        return scraped_update_date.date()

    def grab_data(self) -> pd.DataFrame:
        df = super().grab_data()
        df['service_summary'] = self.service_summary
        return df


cos_scraper = COS_Scraper(
    source="CareerOneStop",
    data_url='https://www.careeronestop.org/TridionMultimedia'
    '/tcm24-49673_XLS_AJC_Data_11172020.xls',
    data_page_url='https://www.careeronestop.org/Developers/Data/data-downloads.aspx',
    data_format="XLS",
    extract_usecols=[
        'ID', 'Name of Center', 'Address1', 'Address2',
        'City', 'State', 'Zip Code',
        'Phone', 'Email Address', 'Web Site URL', 'Office Hours',
    ],
    drop_duplicates_columns=[
        'Name of Center', 'Address1',
        'Address2', 'City', 'State', 'Zip Code'
    ],
    rename_columns={
        "Name of Center": "name", "Address1": "address1",
        "City": "city", "State": "state", "Zip Code": "zip",
        "Address2": "address2", "Phone": "phone", 'Email Address': 'contactEmail',
        "Office Hours": "schedules", "Web Site URL": "website"
    },
    service_summary="Employment Assistance",
    check_collection="services",
    dump_collection="tmpCareerOneStop",
    dupe_collection="tmpCareerOneStopFoundDuplicates",
    data_source_collection_name="career_one_stop",
    collection_dupe_field='ID'
)

if __name__ == "__main__":
    client = get_mongo_client()
    cos_scraper.main_scraper(client)
        