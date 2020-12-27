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


class PS_Scraper(BaseScraper):

    def scrape_updated_date(data_page_url):
        resp = super().scrape_updated_date()
        soup = BeautifulSoup(resp, 'html.parser')
        date_string = soup.find('span', {'property':'dct:modified'}).text
        scrape_updated_date = datetime.strptime(date_string, '%B %d, %Y')
        return scrape_updated_date.date()


    def grab_data(self):
        '''So, for Pittsburgh data, Program_or_Facility will be name,
        address should be split into  address1, city(default to Pittsburgh),
        state, zip and category would be serviceSummary.
        If there are multiple records with the same name,
        it should just put in one record but with concatenating the category.'''


        df = pd.read_csv(self.data_url, usecols=self.extract_usecols)
        df.drop_duplicates(
            subset=self.drop_duplicates_columns,
            inplace=True,
            ignore_index=True
        )
        df.rename(columns={
        "program_or_facility": "name",
        "category": "serviceSummary"
        }, inplace=True)

        '''Concatenating services for facilities with more than one'''
        df = df.groupby(df['address'], as_index=False).agg({'serviceSummary': '; '.join, 'name': 'first'})
        df[['address1','state', 'zip']] = df['address'].str.extract(r'(.+)([A-Z]{2}).+(\d{5})', expand=True)
        df['city'] = 'Pittsburgh'
        df.drop(['address'], axis=1, inplace=True)
        return df






ps_scraper = PS_Scraper(
    source="PittsburghServicesScraper",
    data_url = 'https://data.wprdc.org/datastore/dump/5a05b9ec-2fbf-43f2-bfff-1de2555ff7d4',
    data_page_url = 'https://catalog.data.gov/dataset/bigburgh-social-service-listings',
    data_format = "CSV",
    extract_usecols = [
                          "program_or_facility", "address", "category"
                      ],
    drop_duplicates_columns = [
                                  "program_or_facility", "address", "category"
                              ],
    rename_columns = {
        "program_or_facility": "name", "address": 'address1',
        "category": "serviceSummary"
    },
    service_summary="",
    check_collection="services",
    dump_collection="tmpPittsburghServices",
    dupe_collection="tmpPittsburghServicesFoundDuplicates",
    data_source_collection_name="pittsburgh_services",
    collection_dupe_field='name'
    )




if __name__ == "__main__":
    scraped_update_date = ps_scraper.scrape_updated_date()
    stored_update_date = ps_scraper.retrieve_last_scraped_date(client)
    if stored_update_date is not None:
        if scraped_update_date < stored_update_date:
            logging.info('No new data. Goodbye...')
            sys.exit()
    ps_scraper.main_scraper(client)