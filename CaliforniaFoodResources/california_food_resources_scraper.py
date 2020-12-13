import os
import sys
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient, errors
from tqdm import tqdm

if __package__:  # if script is being run as a module
    from ..shelterapputils.utils import (
        check_similarity, locate_potential_duplicate,
        insert_services, client
    )
    from ..shelterapputils.base_scraper import BaseScraper
else:  # if script is being run as a file
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


class CFR_Scraper(BaseScraper):
    def scrape_updated_date(self) -> str:
        resp = super().scrape_updated_date()
        soup = BeautifulSoup(resp, 'html.parser')
        date_string = soup.find('span', class_='date')
        print(date_string)
        scraped_update_date = datetime.strptime(
            date_string, '%B %d, %Y'
        )
        print(date_string)
        return scraped_update_date.date()

    def grab_data(self) -> pd.DataFrame:
        df = super().grab_data()
        df['service_summary'] = self.service_summary
        return df


cfr_scraper = CFR_Scraper(
    source="CaliforniaFoodResources",
    data_url="https://controllerdata.lacity.org/api/views/v2mg-qsxf/rows.csv",
    data_page_url='https://controllerdata.lacity.org/dataset'
    '/Food-Resources-in-California/v2mg-qsxf',
    data_format="CSV",
    extract_usecols=[
        "Name", "Street Address", "City", "State", "Zip Code",
        "County", "Phone", "Resource Type", "Web Link"
    ],
    drop_duplicates_columns=[
        "Name", "Street Address", "City", "State",
        "Zip Code", "County", "Phone"
    ],
    rename_columns={
        "Name": "name", "Street Address": "address",
        "City": "city", "State": "state", "Zip Code": "zip",
        "County": "county", "Phone": "phone",
        "Resource Type": "resource_type", "Web Link": "url"
    },
    service_summary="Food Pantry",
    check_collection="services",
    dump_collection="tmpCaliforniaFoodResources",
    dupe_collection="tmpCaliforniaFoodResourcesFoundDuplicates",
    data_source_collection_name="california_food_resources",
    collection_dupe_field='name'
)


if __name__ == "__main__":
    # Having trouble scraping the update date from this website because
    # javascript has to run to populate it, which requests doesn't like
    #
    # scraped_update_date = cfr_scraper.scrape_updated_date()
    # stored_update_date = cfr_scraper.retrieve_last_scraped_date()
    # if stored_update_date is not False:
    #     if scraped_update_date < stored_update_date:
    #         print('No new data. Goodbye...')
    #         sys.exit()
    cfr_scraper.main_scraper(client)
