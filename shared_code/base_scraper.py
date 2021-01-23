from typing import List, Any
import logging

from datetime import datetime
import requests
import pandas as pd
from pymongo import MongoClient, errors
from tqdm import tqdm
from pytz import timezone

from dateutil.parser import parse

from shared_code.utils import (
    insert_services, locate_potential_duplicate,
    check_similarity, refresh_ngrams
)

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

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
        """
        Get html for the page. 
        
        Note: Script can only generate static html code (code seen using view source code option) but 
        won't produce javascript generated html content for the page which browser displays.
        """
        return requests.get(self.data_page_url, timeout=(6.05, 15)).text

    def retrieve_last_scraped_date(self, client) -> datetime.date:        
        data_source = client['data-sources'].find_one(
            {"name": self.data_source_collection_name})
        if data_source is None or data_source['last_scraped'] is None: 
            return None
        return parse(data_source['last_scraped']).date()

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
        logger.info(f'initial shape: {df.shape}')
        df.drop_duplicates(
            subset=self.drop_duplicates_columns,
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
        for i, row in tqdm(df.iterrows()):
            idx = df.loc[i, self.collection_dupe_field]
            dupe = coll.find_one(
                {self.collection_dupe_field: idx}
            )
            if dupe is not None:
                found_duplicates.append(i)
        duplicate_df = df.loc[found_duplicates].reset_index(drop=True)
        insert_services(duplicate_df.to_dict('records'), client, self.dupe_collection)
        df = df.drop(found_duplicates).reset_index(drop=True)
        return df

    def is_new_data_available(self, client: MongoClient) -> bool:
        """
        Common routine to check if new data is available for the scraper. 
        """
        scraped_update_date = self.scrape_updated_date()
        stored_update_date = self.retrieve_last_scraped_date(client)
        if stored_update_date is not None:
            if scraped_update_date < stored_update_date:                
                return False
        return True

    def validate_data(self, df):
        #Check if all the required columns ('name', 'address1', 'city', 'state', 'zip') are in the dataframe
        requiredColumns = ['name', 'address1', 'city', 'state', 'zip']
        missingColumns = []
        for column in requiredColumns:
            if not column in df.columns:
                missingColumns.append(column)
        if len(missingColumns) > 0:
            logger.info("The following column(s) are missing from the dataframe: " + str(missingColumns))
            return False

        nullValues = pd.isna(df)
        goodSummaries = {'Emergency Shelter', 'Support Services', 'Food Bank', 'Food Pantry', 'Soup Kitchen',
                         'Legal Assistance', 'Medical Clinic', 'Library', 'Computers', 'Internet', 'Books',
                         'Charging Stations', 'Restrooms', 'Senior Resources', 'Rent Assistance',
                         'Free Pregnancy Testing', 'Domestic Violence Shelter', 'Job Training', 'Clothing',
                         'Disabled Resources', 'Employment Assistance', 'Substance Abuse Treatment',
                         'Transitional Housing', 'Financial Assistance', 'Mental Health Services'}

        for index, row in df.iterrows():
            foundBadData = False
            cityMissing = False
            stateMissing = False
            zipMissing = False

            #Check if the row called 'name' is not null or 'NONE'
            if nullValues['name'][index]:
                logger.info(" row " + str(index + 1) + ": \'name\' cannot be a null value")
                foundBadData = True
            elif row['name'].upper() == "NONE":
                logger.info(" row " + str(index + 1) + ": \'name\' cannot be \'" + row['name'] + "\'")
                foundBadData = True

            #Check if the row called 'city' is not null or 'NONE'
            if nullValues['city'][index]:
                logger.info(" row " + str(index + 1) + ": \'city\' cannot be a null value")
                cityMissing = True
            elif row['city'].upper() == 'NONE':
                logger.info(" row " + str(index + 1) + ": \'city\' cannot be \'" + row['city'] + "\'")
                cityMissing = True

            # Check if the row called 'state' is not null or 'NONE'
            if nullValues['state'][index]:
                logger.info(" row " + str(index + 1) + ": \'state\' cannot be a null value")
                stateMissing = True
            elif row['state'].upper() == 'NONE':
                logger.info(" row " + str(index + 1) + ": \'state\' cannot be \'" + row['state'] + "\'")
                stateMissing = True

            # Check if the row called 'zip' is not null or 'NONE'
            if nullValues['zip'][index]:
                logger.info(" row " + str(index + 1) + ": \'zip\' cannot be a null value")
                zipMissing = True
            elif str(row['zip']).upper() == 'NONE':
                logger.info(" row " + str(index + 1) + ": \'zip\' cannot be \'" + row['zip'] + "\'")
                zipMissing = True

            # Check if the row called 'serviceSummary' is not null or 'NONE'
            if nullValues['serviceSummary'][index]:
                logger.info(" row " + str(index + 1) + ": \'serviceSummary\' was a null value but must be one of the " +
                            "following: " + str(goodSummaries))
                foundBadData = True
            elif row['serviceSummary'] not in goodSummaries:
                logger.info(" row " + str(index + 1) + ": \'serviceSummary\' got " + row['serviceSummary'] +
                            " but must be one of the following: " + str(goodSummaries))
                foundBadData = True

            #The row should be removed if at least one of the following is true:
                #The 'name' column is missing
                #The 'city', 'state', and 'zip' columns are all either null or 'NONE'
                #The 'serviceSummary' column is missing
            if cityMissing and stateMissing and zipMissing:
                foundBadData = True
            if foundBadData:
                df.drop(index, inplace=True)

        return True

    def add_required_fields(self, df: pd.DataFrame):
        """
        Add (if doesn't exists) some required fields in documents to be inserted in db collection. e.g. notes.  
        """
        if not 'notes' in df:
            df['notes'] = ''
        if not 'source' in df:
           if self.source is not None and self.source != '':
                df['source'] = self.source
           else:
                raise Exception("value for field `source` can't be null or emtpy.")

    def main_scraper(self, client: MongoClient) -> None:
        """Base function for ingesting raw data, preparing it and depositing it in MongoDB

        Args:
            client (MongoClient): connection to the MongoDB instance
            scraper_config (ScraperConfig): instance of the ScraperConfig class
        """
        if not self.is_new_data_available(client):
            logger.info('No new data. Goodbye...')
            return

        df = self.grab_data()
        # Only add the data if the dataframe contains each of the following fields: name, address1, city, state, zip
        if not self.validate_data(df):
            return
        if client[self.dump_collection].estimated_document_count() > 0:
            logger.info(f'purging duplicates from existing {self.source} collection')
            df = self.purge_collection_duplicates(df, client)
        if client[self.check_collection].estimated_document_count() == 0:
            # No need to check for duplicates in an empty collection
            insert_services(df.to_dict('records'), client, self.dump_collection)
        else:
            logger.info('refreshing ngrams')
            refresh_ngrams(client, self.check_collection)
            found_duplicates = []
            logger.info('checking for duplicates in the services collection')
            for i, row in tqdm(df.iterrows()):
                dc = locate_potential_duplicate(
                    df.loc[i, 'name'], df.loc[i, 'zip'], client, self.check_collection
                )
                if dc is not False:
                    if check_similarity(df.loc[i, 'name'], dc):
                        found_duplicates.append(i)
            duplicate_df = df.loc[found_duplicates].reset_index(drop=True)
            if len(duplicate_df) > 0:
                logger.info(
                    f'inserting services dupes into the {self.source} dupe collection'
                )
                insert_services(
                    duplicate_df.to_dict('records'), client, self.dupe_collection
                )
            df = df.drop(found_duplicates).reset_index(drop=True)
            logger.info(f'final df shape: {df.shape}')
            self.add_required_fields(df)
            if len(df) > 0:
                insert_services(df.to_dict('records'), client, self.dump_collection)
                logger.info('updating last scraped date in data-sources collection')
                client['data-sources'].update_one(
                    {"name": self.data_source_collection_name},
                    {'$set': {'last_scraped': datetime.now(timezone('UTC')).replace(microsecond=0).isoformat()}},
                    upsert=True
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
