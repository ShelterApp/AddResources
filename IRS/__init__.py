import logging
import json
import os

import datetime
from pymongo import MongoClient, TEXT
import azure.functions as func

from .irs_scraper import main as start


def main(mytimer: func.TimerRequest, context: func.Context) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    with open(context.function_directory + '/config.json', 'r') as con:
        config = json.load(con)

    conn_string = os.environ['MONGO_DB_CONNECTION_STRING']

    client = MongoClient(conn_string)['shelter']

    start(config, client, 'services', 'tmpIRS', 'tmpIRSFoundDuplicates')

    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info(f'Python timer trigger function for IRS Scraping ran at utc: {utc_timestamp}')
