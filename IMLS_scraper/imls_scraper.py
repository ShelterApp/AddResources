import os
import sys
import fnmatch
from datetime import datetime
from typing import List, Any
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
from pymongo import MongoClient, errors
from tqdm import tqdm
import urllib 

_i = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ["DBUSERNAME"] = "democracylab"
os.environ["PW"] = "DemocracyLab2019"
if _i not in sys.path:
    # add parent directory to sys.path so utils module is accessible
    sys.path.insert(0, _i)
del _i  # clean up global name space
from shared_code.utils import (
    check_similarity, locate_potential_duplicate,
    insert_services, client
)
from shared_code.base_scraper import BaseScraper

     
class IMLS_Scraper(BaseScraper):
  
    def __init__(self,
                 source: str,
                 data_url: str,
                 data_page_url: str,
                 data_format: str,
                 extract_usecols: List[str],
                 drop_duplicates_columns: List[str],
                 rename_columns: dict,
                 service_summary: Any,
                 check_collection: str,
                 dump_collection: str,
                 dupe_collection: str,
                 data_source_collection_name: str,
                 collection_dupe_field: str,
                 encoding: str = 'utf-8'):
        BaseScraper.__init__(self, source, data_url, data_page_url, data_format, extract_usecols, drop_duplicates_columns, rename_columns, service_summary, check_collection, dump_collection, dupe_collection, data_source_collection_name, collection_dupe_field, encoding)
        resp = super().scrape_updated_date()
        soup = BeautifulSoup(resp, 'html.parser')
        latestLink = ""
        latestDate = 0
        for link in soup.findAll('a', href=True, text='CSV'):
             match = re.match(r'.*([1-3][0-9]{3})', link['href'])
             if match is not None:
                year = int(match.group(1))
                if year > latestDate:
                    latestDate = year
                    latestLink = link['href']

        self._data_url: str = latestLink
        self._latest_date: datetime = datetime.strptime(str(latestDate), '%Y')

    def scrape_updated_date(self) -> str:
        return self.latest_date.strftime('%Y-%m-%d')

    def grab_data(self) -> pd.DataFrame:      
        r = requests.get(self.data_url)
        zf = ZipFile(BytesIO(r.content))
        for info in zf.infolist():
            if fnmatch.fnmatch(info.filename, '*_outlet_*.csv'):
                df = pd.read_csv(zf.open(info.filename),         
                usecols=self.extract_usecols,
                encoding = "ISO-8859-1")
                df = super().grab_data(df=df)
                return df

    @property
    def latest_date(self) -> datetime:
        return self._latest_date


imls_scraper = IMLS_Scraper(
    source="IMLS",
    data_url='https://placeholderurl',
    data_page_url='https://www.imls.gov/research-evaluation/data-collection/public-libraries-survey',
    data_format="DF",
    extract_usecols=[
       'LIBID', 'LIBNAME', 'ADDRESS', 'CITY', 'STABR', 'ZIP', 'PHONE'
    ],
    drop_duplicates_columns=[
       'LIBNAME', 'ADDRESS', 'CITY', 'STABR', 'ZIP'
    ],
    rename_columns={
        "STABR": "state", "LIBNAME": "name", "ZIP": "zip"
    },
    service_summary="Computers, Internet, Books, Charging Stations, Restrooms",
    check_collection="services",
    dump_collection="tmpIMLS",
    dupe_collection="tmpIMLSDuplicates",
    data_source_collection_name="imls",
    collection_dupe_field='LIBNAME'
)


if __name__ == "__main__":
    imls_scraper.main_scraper(client)
