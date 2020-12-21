from typing import List, Any
from datetime import datetime

import requests
import pandas as pd
from pymongo import MongoClient, errors
from tqdm import tqdm

from shelterapputils.utils import (
    insert_services, locate_potential_duplicate,
    check_similarity, refresh_ngrams
)


class BaseScraper:
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
                 encoding: str = 'utf-8') -> None:
        self._source: str = source
        self._data_url: str = data_url
        self._data_page_url: str = data_page_url
        self._data_format: str = data_format
        self._encoding: str = encoding
        self._extract_usecols: List[str] = extract_usecols
        self._drop_duplicates_columns: List[str] = drop_duplicates_columns
        self._rename_columns: dict = rename_columns
        self._service_summary: Any = service_summary
        self._check_collection: str = check_collection
        self._dump_collection: str = dump_collection
        self._dupe_collection: str = dupe_collection
        self._data_source_collection_name: str = data_source_collection_name
        self._collection_dupe_field: str = collection_dupe_field

    def scrape_updated_date(self):
        return requests.get(self.data_page_url, timeout=(6.05, 15)).text

    def retrieve_last_scraped_date(self, client):
        try:
            stored_update_date = client['data-sources'].find_one(
                {"name": self.data_source_collection_name}
            )['last_updated']
            stored_update_date = datetime.strptime(
                str(stored_update_date), '%Y-%m-%d %H:%M:%S'
            ).date()
        except Exception as e:
            print(e)
        if stored_update_date is not False:
            return stored_update_date
        return False

    def grab_data(self, df=None) -> pd.DataFrame:
        """Base function for retrieving raw data and performing basic pre-processing

        Returns:
            pd.DataFrame: DataFrame of pre-processed data
        """
        if self.data_format == "JSON":
            df: pd.DataFrame = pd.read_json(self.data_url)
        elif self.data_format == "CSV":
            df = pd.read_csv(
                self.data_url, encoding=self.encoding, usecols=self.extract_usecols
            )
        elif self.data_format == "DF":
            df = df
        else:
            df = pd.read_excel(self.data_url, usecols=self.extract_usecols)
        print(f'initial shape: {df.shape}')
        df.drop_duplicates(
            subset=list(self.drop_duplicates_columns),
            inplace=True,
            ignore_index=True
        )
        df.rename(columns=self.rename_columns, inplace=True)
        # One-Liner to trim all the strings in the DataFrame
        df.applymap(lambda x: x if not x or not isinstance(x, str) else x.strip())
        if 'zip' in list(df.columns):
            df['zip'] = df['zip'].astype("str")
            df['zip'] = df['zip'].apply(
                lambda z: z[0:5] if "-" in z else z
            )
        df['source'] = [self.source] * len(df)
        return df

    def purge_collection_duplicates(
        self, df: pd.DataFrame, client: MongoClient
    ) -> pd.DataFrame:
        """Function to check the pre-processed data and
        delete exact dupes that already exist in the tmp collection

        Args:
            df (pd.DataFrame): pre-processed data from grab_data()
            client (MongoClient): MongoDB connection instance

        Returns:
            pd.DataFrame: DataFrame free of exact duplicates
        """
        found_duplicates = []
        coll = client[self.dump_collection]
        for i in range(len(df)):
            idx = int(df.loc[i, self.collection_dupe_field])
            dupe = coll.find_one(
                {self.collection_dupe_field: idx}
            )
            if dupe is not False:
                found_duplicates.append(i)
        duplicate_df = df.loc[found_duplicates].reset_index(drop=True)
        insert_services(duplicate_df.to_dict('records'), client, self.dupe_collection)
        df = df.drop(found_duplicates).reset_index(drop=True)
        return df

    def main_scraper(self, client: MongoClient) -> None:
        """Base function for ingesting raw data, preparing it and depositing it in MongoDB

        Args:
            client (MongoClient): connection to the MongoDB instance
            scraper_config (ScraperConfig): instance of the ScraperConfig class
        """
        df = self.grab_data()
        if client[self.dump_collection].estimated_document_count() > 0:
            print(f'purging duplicates from existing {self.source} collection')
            df = self.purge_collection_duplicates(df)
        if client[self.check_collection].estimated_document_count() == 0:
            # No need to check for duplicates in an empty collection
            insert_services(df.to_dict('records'), client, self.dump_collection)
        else:
            print('refreshing ngrams')
            refresh_ngrams(client, self.check_collection)
            found_duplicates = []
            print('checking for duplicates in the services collection')
            for i in tqdm(range(len(df))):
                dc = locate_potential_duplicate(
                    df.loc[i, 'name'], df.loc[i, 'zip'], client, self.check_collection
                )
                if dc is not False:
                    if check_similarity(df.loc[i, 'name'], dc):
                        found_duplicates.append(i)
            duplicate_df = df.loc[found_duplicates].reset_index(drop=True)
            if len(duplicate_df) > 0:
                print(f'inserting services dupes into the {self.source} dupe collection')
                insert_services(
                    duplicate_df.to_dict('records'), client, self.dupe_collection
                )
            df = df.drop(found_duplicates).reset_index(drop=True)
            print(f'final df shape: {df.shape}')
            if len(df) > 0:
                insert_services(df.to_dict('records'), client, self.dump_collection)
                print('updating scraped update date in data-sources collection')
                client['data_sources'].update_one(
                    {"name": self.data_source_collection_name},
                    {'$set': {'last_updated': datetime.strftime(datetime.now(), '%m/%d/%Y')}}
                )

    @property
    def source(self) -> str:
        return self._source

    @property
    def data_url(self) -> str:
        return self._data_url

    @property
    def data_page_url(self) -> str:
        return self._data_page_url

    @property
    def data_format(self) -> str:
        return self._data_format

    @property
    def encoding(self) -> str:
        return self._encoding

    @property
    def extract_usecols(self) -> List[str]:
        return self._extract_usecols

    @property
    def drop_duplicates_columns(self) -> List[str]:
        return self._drop_duplicates_columns

    @property
    def rename_columns(self) -> dict:
        return self._rename_columns

    @property
    def service_summary(self) -> str:
        return self._service_summary

    @property
    def check_collection(self) -> str:
        return self._check_collection

    @property
    def dump_collection(self) -> str:
        return self._dump_collection

    @property
    def dupe_collection(self) -> str:
        return self._dupe_collection

    @property
    def data_source_collection_name(self) -> str:
        return self._data_source_collection_name

    @property
    def collection_dupe_field(self) -> str:
        return self._collection_dupe_field
