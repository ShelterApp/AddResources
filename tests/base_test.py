import json
from bson import ObjectId
import os

import datetime
import requests
import pytest
import mongomock
from pymongo import MongoClient, TEXT

from shelterapputils.utils import (
    make_ngrams, distance, insert_services, check_similarity,
    locate_potential_duplicate, refresh_ngrams
)
from shelterapputils.base_scraper import BaseScraper


@pytest.fixture
def mock_mongo_client():
    return mongomock.MongoClient()


@pytest.fixture
def base_scraper_fixture():
    return BaseScraper(
        source='mock',
        data_url='https://mockurl.mock',
        data_page_url='https://mockurl.mock',
        data_format='CSV',
        extract_usecols=[
            "Name", "Street Address", "City", "State", "Zip Code",
            "County", "Phone", "Resource Type", "Web Link"
        ],
        drop_duplicates_columns=[
            "Name", "Street Address", "City", "State",
            "Zip Code", "County", "Phone"
        ],
        rename_columns={
            "Name": "name", "Street Address": "address",
            "City": "city", "State": "state", "Zip Code": "zip",
            "County": "county", "Phone": "phone",
            "Resource Type": "resource_type", "Web Link": "url"
        },
        service_summary='resource_type',
        check_collection='services',
        dump_collection='tmpTestingMock',
        dupe_collection='tmpTestingMockFoundDuplicates',
        data_source_collection_name='testingmock',
        collection_dupe_field='Name'
    )


@pytest.fixture
def example_data_source_collection():
    return [{
        '_id': ObjectId('5f137b06236870dae2f1f5e4'),
        'name': 'testingmock',
        'last_updated': datetime.datetime(2019, 11, 20)
    },
        {
        '_id': ObjectId('5f7a5ab32410168bf92a4ca1'),
        'name': 'othermock',
        'last_updated': datetime.datetime(2019, 11, 20)
    }]


def substitute_get_request(data_page_url, timeout=(6.05, 15)):
    return '''
        <div class="panel panel-primary">
        <div class="panel-heading">
        <div class="panel-title">About this Record</div>
        </div>
        <ul class="list-group">
        <li class="list-group-item">
            <strong>Record Released:</strong>
            2017-09-26
        </li>
        <li class="list-group-item">
            <strong>Record Modified:</strong>
            2020-06-22
        </li>
        <li class="list-group-item">
            <strong>Record ID:</strong>
            7e0189e3-8595-4e62-a4e9-4fed6f265e10
        </li>
        <li class="list-group-item">
            <strong>Metadata:</strong>
            <ul class="list-group" style="margin-bottom: 0;">
            <li class="list-group-item">
                <a href="https://open.canada.ca/data/api/action/package_show?id=7e0189e3-8595-
                4e62-a4e9-4fed6f265e10" rel="nofollow">
                Link to JSON format
                </a>
            </li>
            <li class="list-group-item" style="border: none;">
                <a href="/data/en/dataset/7e0189e3-8595-4e62-a4e9-4fed6f265e10.jsonld"
                rel="nofollow">
                DCAT (JSON-LD)
                </a>
            </li>
            <li class="list-group-item" style="border: none;">
                <a href="/data/en/dataset/7e0189e3-8595-4e62-a4e9-4fed6f265e10.xml"
                rel="nofollow">
                DCAT (XML)
                </a>
            </li>
            </ul>
        </li>
        <li class="list-group-item">
            <a href="/data/en/dataset/history/7e0189e3-8595-4e62-a4e9-4fed6f265e10?format=
            atom" title="RSS">
            <span class="fa fa-rss-square"></span> Atom Feed
            </a>
        </li>
        </ul>
        </div>
    '''


def test_scrape_updated_date(monkeypatch, base_scraper_fixture):
    monkeypatch.setattr(BaseScraper, 'scrape_updated_date', substitute_get_request)
    resp = base_scraper_fixture.scrape_updated_date()
    assert type(resp) == str


def test_retrieve_last_scraped_date(
    mock_mongo_client, example_data_source_collection, base_scraper_fixture
):
    insert_services(example_data_source_collection, mock_mongo_client.shelter, 'data-sources')
    stored_date = base_scraper_fixture.retrieve_last_scraped_date(mock_mongo_client.shelter)
    assert type(stored_date) == datetime.date
