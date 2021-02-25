import mongomock
import pytest
from dotenv import load_dotenv
import os
from shared_code.utils import (
    make_ngrams, distance, insert_services, check_similarity,
    locate_potential_duplicate, refresh_ngrams, get_mongo_client
)

from pymongo import MongoClient

'''
Assumes ngram and distance works as proved in IRS_test.py
'''
load_dotenv()

@pytest.fixture
def mock_mongo_client():
    return mongomock.MongoClient()

@pytest.fixture
def shelter_db():
    return MongoClient("mongodb+srv://" + os.environ.get("DB_USER") + ":" + os.environ.get("DB_PASS") + "@shelter-rm3lc.azure.mongodb.net/shelter?retryWrites=true&w=majority")['shelter']

@pytest.fixture
def test_scraper_data():
    return [{"_id":"6017e08548cc6486984de5f6","name":"Baxley City Gym","notes":"Open","address1":"69 Tippins Street Baxley, GA 31513","city":"Baxley","state":"GA","zip":"31513","phone":"(912)-339-0072","country":"US","source":"summer_meal_sites"},
     {"_id":"6017e08548cc6486984de5d5","name":"Crossroads Baptists Church","notes":"Open","address1":"243 Marshall Mill Road Fort Valley, GA 31030","city":"Fort Valley","state":"GA","zip":"31030","phone":"(478)-836-4546","country":"US","source":"summer_meal_sites"},
     {"_id":"6017e08548cc6486984de5e0","name":"Crossroads Community Parks","notes":"Open","address1":"243 Marshall Mill Road Fort Valley, GA 31030","city":"Fort Valley","state":"GA","zip":"31030","phone":"(478)-836-4546","country":"US","source":"summer_meal_sites"}]

@pytest.fixture
def test_services_data():
    return [{"_id":"6017e08548cc6486984de5a2","name":"Crossroads Baptist Church","notes":"Open","address1":"243 Marshall Mill Road Fort Valley, GA 31030","city":"Fort Valley","state":"GA","zip":"31030","phone":"(478)-836-4546","country":"US","source":"summer_meal_sites"},
     {"_id":"6017e08548cc6486984de5a3","name":"Crossroads Community Park","notes":"Open","address1":"243 Marshall Mill Road Fort Valley, GA 31030","city":"Fort Valley","state":"GA","zip":"31030","phone":"(478)-836-4546","country":"US","source":"summer_meal_sites"}]


def test_insert_services(test_scraper_data, mock_mongo_client):
    insert_services(test_scraper_data, mock_mongo_client.shelter, 'tmpTestScraper')
    found_services = [obj for obj in mock_mongo_client.shelter.tmpTestScraper.find({})]
    assert test_scraper_data == found_services


def test_refresh_ngrams(test_services_data,mock_mongo_client):
    insert_services(test_services_data, mock_mongo_client.shelter, 'services')
    refresh_ngrams(mock_mongo_client.shelter,'services')
    assert mock_mongo_client.shelter.services.find_one({"name":"Crossroads Baptist Church"})["ngrams"] == ' '.join(make_ngrams('Crossroads Baptist Church'.upper()))

def test_check_similarity():
    assert distance('Crossroads Baptists Church', 'Crossroads Baptist Church') >= .9
    assert check_similarity('Crossroads Baptists Church', 'Crossroads Baptist Church')


def test_locate_potential_duplicate(test_scraper_data,test_services_data, shelter_db):
    tmp_test_scraper_coll = shelter_db['tmpTestScraper']
    test_services_coll = shelter_db['testServices']

    try:
        test_services_coll.insert_many(test_services_data)
        refresh_ngrams(shelter_db,'testServices')

        #Crossroads Baptist Church
        doc = test_scraper_data[1]
        assert check_similarity(doc['name'], test_services_data[0]['name'])
        assert locate_potential_duplicate(doc['name'], doc['zip'], shelter_db, 'testServices')



        doc = test_scraper_data[2]
        ngrams_match = False
        for ngram in make_ngrams(doc['name']):
            if ngram in make_ngrams(test_services_data[0]['name']):
                ngrams_match = True
                break

        assert ngrams_match

        assert check_similarity(doc['name'], test_services_data[1]['name'])

        assert not check_similarity(doc['name'], test_services_data[0]['name'])

        located_dupe_name = locate_potential_duplicate(doc['name'], doc['zip'], shelter_db, 'testServices')
        assert located_dupe_name
        assert check_similarity(located_dupe_name, doc['name'])

    except Exception as e:
        test_services_coll.drop()
        raise(e)


    test_services_coll.drop()
