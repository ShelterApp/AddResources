import logging
import json
import os
import sys

import datetime
from pymongo import MongoClient, TEXT
import azure.functions as func

from .nyc_runaway_and_homeless_youth import nyc_youth_scraper


def main(mytimer: func.TimerRequest, context: func.Context) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc
    ).isoformat()
    conn_string = os.environ['MONGO_DB_CONNECTION_STRING']
    client = MongoClient(conn_string)['shelter']
    if stored_update_date is not None:
        if scraped_update_date < stored_update_date:
            logging.info('No new NYC Youth Centers data. Goodbye...')
            sys.exit()
    nyc_youth_scraper.main_scraper(client)
    if mytimer.past_due:
        logging.info('The timer is past due!')
    logging.info(f'Python timer trigger function for NYC Youth Centers Scraping ran at utc: {utc_timestamp}')
