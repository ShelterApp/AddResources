import os
import sys
import pandas as pd
import logging
import pytest
from shared_code.utils import validate_data

logger = logging.getLogger(__name__)

_i = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _i not in sys.path:
    # add parent directory to sys.path so utils module is accessible
    sys.path.insert(0, _i)
del _i  # clean up global name space


# This test checks if the script validator will throw an exception if there are any rows with invalid data
def test_validator_on_rows():
    df = pd.read_csv('tests/test_data_validation_before_insert.csv')
    check_validator(df, False, 'Some rows have invalid data. Check logs for details.')


# This test checks if the script validator will throw an exception if any of the necessary columns are missing
def test_validator_on_columns():
    lst = [['Name0', 'Address0'], ['Name1', 'Address1'], ['Name2', 'Address2'], ['Name3', 'Address3']]
    df = pd.DataFrame(lst, columns=['name', 'address1'])
    check_validator(df, False, 'The data frame does not contain the following required column(s): ')


# This test checks if the script validator will throw an exception if there are duplicated rows in terms of the columns
# 'name', 'address1', 'city', 'state', 'zip', and 'serviceSummary'
def test_validator_on_duplicates_with_serviceSummary():
    df = pd.read_csv('tests/test_data_for_combine_serviceSummary_column.csv')
    check_validator(df, False, 'There are duplicate rows in the data. Check logs for details.')


# This test checks if the script validator will throw an exception if there are duplicated rows in terms of the columns
# 'name', 'address1', 'city', 'state', and 'zip', except 'serviceSummary'
def test_validator_on_duplicates():
    df = pd.read_csv('tests/test_data_validation_duplicates.csv')
    check_validator(df, False, 'There are duplicate entries in data which only differ in the serviceSummary field ' +
                    'and hence can be combined into single entry.')


# This test checks if the script validator will not throw an exception if none of the rows contain invalid data
# and if all the necessary columns are present
def test_validator_valid_data():
    df = pd.read_csv('tests/test_data_validation_correct.csv')
    check_validator(df, True, 'All the data in this data frame is valid.')


def check_validator(df, meant_to_be_valid, message):
    try:
        validate_data(df)
        assert meant_to_be_valid
    except Exception as e:
        assert not meant_to_be_valid
        assert str(e).startswith(message)
