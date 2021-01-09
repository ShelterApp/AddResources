import os
import sys
import logging
import re
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
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


class CanadaSheltersScraper(BaseScraper):

    def scrape_updated_date(self):
        resp = super().scrape_updated_date()
        soup = BeautifulSoup(resp, 'html.parser')
        table_rows = soup.find_all('li', {'class': 'list-group-item'})
        for r in table_rows:
            if r.find('strong', text='Record Modified:'):
                date_string = re.search(
                    r'(19|20)\d\d[- /.](0[1-9]|1[012])[- /.](0[1-9]|[12][0-9]|3[01])',
                    r.text
                )
        date_string = date_string.group(0)
        scraped_update_date = datetime.strptime(
            date_string, '%Y-%m-%d'
        )
        return scraped_update_date.date()

    def grab_data(self) -> pd.DataFrame:
        df = super().grab_data()
        return df

data_source_name = "canada_shelters"

CSS = CanadaSheltersScraper(
    source=data_source_name,
    data_url='http://www.edsc-esdc.gc.ca/ouvert-open/hps/'
    'FINAL_CHPDOpenDataNSPL_Dataset-2019_June7_2020.csv',
    data_page_url='https://open.canada.ca/data/en/dataset/'
    '7e0189e3-8595-4e62-a4e9-4fed6f265e10',
    data_format="CSV",
    extract_usecols=[
        'Shelter Name/Nom du refuge',
        'City/Ville', 'Province Code',
        'Shelter Type'],
    drop_duplicates_columns=[
        'Shelter Name/Nom du refuge',
        'City/Ville', 'Province Code',
        'Shelter Type'],
    rename_columns={
        'Shelter Name/Nom du refuge': 'name',
        'City/Ville': 'city',
        'Province Code': 'state',
        'Shelter Type': 'serviceSummary'},
    service_summary="Shelter",
    check_collection="services",
    dump_collection="tmpCanadaShelters",
    dupe_collection="tmpCanadaSheltersDuplicates",
    data_source_collection_name=data_source_name,
    collection_dupe_field='name',
    encoding='latin-1'
)

if __name__ == "__main__":
    CSS.main_scraper(client)

    
