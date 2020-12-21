import os
import sys
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
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
from shelterapputils.scraper_utils import main_scraper


class HUD_Scraper(BaseScraper):
    def scrape_updated_date(self) -> str:
        resp = super().scrape_updated_date()
        update_statement = re.search(
            r'Date Published: (\w{3,8} \d{4})', resp
        )
        scraped_date = datetime.strptime(
            update_statement.group(1), "%B %Y"
        ).date()
        return scraped_date

    def grab_data(self) -> pd.DataFrame:
        df = super().grab_data()
        df['service_type'] = ['SHELTER'] * len(df)
        service_summaries = self.service_summary
        for i in tqdm(range(len(df))):
            if df.loc[i, 'Project Type'] == 'ES':
                if df.loc[i, 'Target Population'] is not None:
                    service_summaries.append('Domestic Violence Shelter')
                else:
                    service_summaries.append('Emergency Shelter')
            else:
                service_summaries.append('Transitional Housing')
        df['service_summary'] = service_summaries
        return df


hud_scraper = HUD_Scraper(
    source='HUD',
    data_url='https://www.huduser.gov/portal/sites/default/files/xls/2019'
    '-Housing-Inventory-County-RawFile.xlsx',
    data_page_url='https://www.hudexchange.info/resource/3031/pit-and-hic-data-since-2007/',
    data_format='XLS',
    extract_usecols=[
        'Row #', 'Project ID', 'Target Population',
        'Project Name', 'Project Type',
        'Bed Type', 'Victim Service Provider', 'address1',
        'address2', 'city', 'state', 'zip', 'Total Beds', 'Updated On'
    ],
    drop_duplicates_columns=[
        'Organization Name', 'Project Name', 'address1', 'city', 'state', 'zip'
    ],
    rename_columns={
        'Project Name': 'name',
        'Address2': 'address2', 'Phone': 'phone',
    },
    service_summary=[],
    check_collection='services',
    dump_collection='tmpHUD',
    dupe_collection='tmpHUDFoundDuplicates',
    data_source_collection_name='hud_pit_hic_data',
    collection_dupe_field='Project ID'
)


if __name__ == "__main__":
    scraped_update_date = hud_scraper.scrape_updated_date()
    stored_update_date = hud_scraper.retrieve_last_scraped_date()
    if stored_update_date is not False:
        if scraped_update_date < stored_update_date:
            print('No new data. Goodbye...')
            sys.exit()
    hud_scraper.main_scraper(client)
