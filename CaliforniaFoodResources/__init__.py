import logging
import json
import os
import sys

import datetime
from pymongo import MongoClient, TEXT
import azure.functions as func

from .california_food_resources_scraper import cfr_scraper


def main(mytimer: func.TimerRequest, context: func.Context) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc
    ).isoformat()
    conn_string = os.environ['MONGO_DB_CONNECTION_STRING']
    client = MongoClient(conn_string)['shelter']
    # scraped_update_date = cfr_scraper.scrape_updated_date()
    # stored_update_date = cfr_scraper.retrieve_last_scraped_date(client)

    # if stored_update_date is not None:
    #     if scraped_update_date < stored_update_date:
    #         logging.info('No new CFR data. Goodbye...')
    #         sys.exit()
    cfr_scraper.main_scraper(client)
    if mytimer.past_due:
        logging.info('The timer is past due!')
    logging.info(f'Python timer trigger function for CFR Scraping ran at utc: {utc_timestamp}')
