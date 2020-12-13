from datetime import datetime

import pandas as pd
from pymongo import MongoClient, errors
from tqdm import tqdm

from shelterapputils.scraper_config import ScraperConfig
from shelterapputils.utils import insert_services, locate_potential_duplicate, check_similarity


def grab_data(scraper_config: ScraperConfig) -> pd.DataFrame:
    """Base function for retrieving raw data and performing basic pre-processing

    Args:
        scraper_config (ScraperConfig): Instance of the ScraperConfig class

    Returns:
        pd.DataFrame: DataFrame of pre-processed data
    """
    if scraper_config.data_format == "JSON":
        df: pd.DataFrame = pd.read_json(scraper_config.data_url)
    elif scraper_config.data_format == "CSV":
        df = pd.read_csv(
            scraper_config.data_url,
            usecols=scraper_config.extract_usecols
        )
    else:
        df = pd.read_excel(
            scraper_config.data_url,
            usecols=scraper_config.extract_usecols
        )
    print(f'initial shape: {df.shape}')
    df.drop_duplicates(
        subset=scraper_config.drop_duplicates_columns,
        inplace=True,
        ignore_index=True
    )
    df.rename(columns=scraper_config.rename_columns, inplace=True)
    # One-Liner to trim all the strings in the DataFrame
    df.applymap(lambda x: x if not x or not isinstance(x, str) else x.strip())
    df['zip'] = df['zip'].astype("str")
    df['zip'] = df['zip'].apply(lambda z: z[0:5] if "-" in z else z)
    df['source'] = [scraper_config.source] * len(df)
    df['service_summary'] = scraper_config.service_summary
    return df


def main_scraper(client: MongoClient, scraper_config: ScraperConfig):
    """Base function for ingesting raw data, preparing it and depositing it in MongoDB

    Args:
        client (MongoClient): connection to the MongoDB instance
        scraper_config (ScraperConfig): instance of the ScraperConfig class
    """
    df = grab_data(scraper_config)
    if client[scraper_config.dump_collection].estimated_document_count() > 0:
        print('purging duplicates from existing CareerOneStop collection')
        df = scraper_config.purge_collection_duplicates(df)
    if client[scraper_config.check_collection].estimated_document_count() == 0:
        # No need to check for duplicates in an empty collection
        insert_services(df.to_dict('records'), client, scraper_config.dump_collection)
    else:
        print('refreshing ngrams')
        # refresh_ngrams(client, check_collection)
        found_duplicates = []
        print('checking for duplicates in the services collection')
        for i in tqdm(range(len(df))):
            dc = locate_potential_duplicate(
                df.loc[i, 'name'], df.loc[i, 'zip'], client, scraper_config.check_collection
            )
            if dc is not False:
                if check_similarity(df.loc[i, 'name'], dc):
                    found_duplicates.append(i)
        duplicate_df = df.loc[found_duplicates].reset_index(drop=True)
        print(f'inserting {duplicate_df.shape[0]} services dupes into the dupe collection')
        if len(duplicate_df) > 0:
            insert_services(
                duplicate_df.to_dict('records'), client, scraper_config.dupe_collection
            )
        df = df.drop(found_duplicates).reset_index(drop=True)
        print(f'final df shape: {df.shape}')
        if len(df) > 0:
            insert_services(df.to_dict('records'), client, scraper_config.dump_collection)
        print('updating scraped update date in data-sources collection')
        try:
            client['data_sources'].update_one(
                {"name": scraper_config.data_source_collection_name},
                {'$set': {'last_updated': datetime.strftime(datetime.now(), '%m/%d/%Y')}}
            )
        except errors.OperationFailure as e:
            print(e)
