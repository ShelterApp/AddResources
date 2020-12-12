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
    from ..shelterapputils.scraper_config import ScraperConfig
    from ..shelterapputils.scraper_utils import main_scraper
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
    from shelterapputils.scraper_config import ScraperConfig
    from shelterapputils.scraper_utils import main_scraper


def scrape_updated_date():
    datasets_url: str = 'https://www.careeronestop.org/Developers/Data/data-downloads.aspx'
    resp = requests.get(datasets_url)
    soup = BeautifulSoup(resp.text, 'html.parser')
    table_rows = soup.find_all('tr')
    found = False
    for r in table_rows:
        for i, d in enumerate(r.find_all('td')):
            if i == 0:
                if d.text.strip() == 'Comprehensive and Affiliate American Job Centers':
                    found = True
            if i == 1 and found is True:
                month_string = d.text
                found = False
    scraped_update_date = datetime.strptime(
        month_string, '%B %Y'
    )
    print(month_string)
    return scraped_update_date.date()


career_one_stop_scraper_config: ScraperConfig = ScraperConfig(
    source="CareerOneStop",
    data_url='https://www.careeronestop.org/TridionMultimedia'
    '/tcm24-49673_XLS_AJC_Data_11172020.xls',
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
        "Name of Center": "name", "Address1": "address",
        "City": "city", "State": "state", "Zip Code": "zip",
        "Address2": "address2", "Phone": "phone", 'Email Address': 'email',
        "Office Hours": "hours", "Web Site URL": "url"
    },
    service_summary="Employment Assistance",
    check_collection="services",
    dump_collection="tmpCareerOneStop",
    dupe_collection="tmpCareerOneStopFoundDuplicates",
    data_source_collection_name="career_one_stop",
    collection_dupe_field='ID'
)


if __name__ == "__main__":
    scraped_update_date = scrape_updated_date()
    try:
        stored_update_date = client['data-sources'].find_one(
            {"name": "career_one_stop"}
        )['last_updated']
        stored_update_date = datetime.strptime(
            str(stored_update_date), '%Y-%m-%d %H:%M:%S'
        ).date()
    except Exception as e:
        print(e)
    if stored_update_date is not False:
        if scraped_update_date < stored_update_date:
            print('No new data. Goodbye...')
            sys.exit()
    main_scraper(client, career_one_stop_scraper_config)
