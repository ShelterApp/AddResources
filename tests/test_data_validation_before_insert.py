import os
import sys
import pandas as pd
import logging
from shared_code.utils import validate_data

logger = logging.getLogger(__name__)

_i = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _i not in sys.path:
    # add parent directory to sys.path so utils module is accessible
    sys.path.insert(0, _i)
del _i  # clean up global name space


# This scrapper tests if the script validator can remove any rows with invalid data
class TestDataScraper():
    def test_validator_on_rows(self):
        df = pd.read_csv('tests/test_data_validation_before_insert.csv')
        validate_data(df)
        logger.info(df)
        logger.info(" The dataframe contains " + str(len(df)) + " valid rows. 13 valid rows were expected.")

    def test_validator_on_columns(self):
        df = pd.read_csv('tests/test_data_wrong_columns.csv')
        validate_data(df)
        logger.info(df)
        logger.info(" The dataframe contains " + str(len(df)) + " valid rows. 0 valid rows were expected.")


pd.set_option("display.max_rows", 13, "display.max_columns", 9)
test_data_scraper = TestDataScraper()
test_data_scraper.test_validator_on_rows()
logger.info('\n')
test_data_scraper.test_validator_on_columns()
