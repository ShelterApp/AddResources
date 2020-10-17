import pytest
from IRS.irs_scraper import (
    make_ngrams, distance, insert_services, check_similarity,
    locate_potential_duplicate, refresh_ngrams
)
import json
from pymongo import MongoClient, TEXT
from bson import ObjectId
import mongomock
import os


@pytest.fixture
def mock_config_object():
    with open('IRS/config.json', 'r') as con:
        config = json.load(con)
        return config


@pytest.fixture
def example_IRS_search_object_with_spelled_out_saint():
    return {
        '_id': ObjectId('5f137b06236870dae2f1f5e4'),
        'STREET': 'PO BOX 376',
        'CITY': 'PR DU CHIEN',
        'NAME': 'SAINT FERIOLE ISLAND PARK',
        'NGRAMS': 'SAINT F AINT FE INT FER NT FERI T FERIO  FERIOL FERIOLE ERIOLE  RIOLE '
                  'I IOLE IS OLE ISL LE ISLA E ISLAN  ISLAND ISLAND  SLAND P LAND PA AND '
                  'PAR ND PARK SAINT FE AINT FER INT FERI NT FERIO T FERIOL  FERIOLE '
                  'FERIOLE  ERIOLE I RIOLE IS IOLE ISL OLE ISLA LE ISLAN E ISLAND  '
                  'ISLAND  ISLAND P SLAND PA LAND PAR AND PARK SAINT FER AINT FERI '
                  'INT FERIO NT FERIOL T FERIOLE  FERIOLE  FERIOLE I ERIOLE IS RIOLE '
                  'ISL IOLE ISLA OLE ISLAN LE ISLAND E ISLAND   ISLAND P ISLAND PA '
                  'SLAND PAR LAND PARK SAINT FERI AINT FERIO INT FERIOL NT FERIOLE T '
                  'FERIOLE   FERIOLE I FERIOLE IS ERIOLE ISL RIOLE ISLA IOLE ISLAN '
                  'OLE ISLAND LE ISLAND  E ISLAND P  ISLAND PA ISLAND PAR SLAND PARK '
                  'SAINT FERIO AINT FERIOL INT FERIOLE NT FERIOLE  T FERIOLE I  '
                  'FERIOLE IS FERIOLE ISL ERIOLE ISLA RIOLE ISLAN IOLE ISLAND OLE '
                  'ISLAND  LE ISLAND P E ISLAND PA  ISLAND PAR ISLAND PARK SAINT '
                  'FERIOL AINT FERIOLE INT FERIOLE  NT FERIOLE I T FERIOLE IS  '
                  'FERIOLE ISL FERIOLE ISLA ERIOLE ISLAN RIOLE ISLAND IOLE '
                  'ISLAND  OLE ISLAND P LE ISLAND PA E ISLAND PAR  ISLAND PARK '
                  'SAINT FERIOLE AINT FERIOLE  INT FERIOLE I NT FERIOLE IS T '
                  'FERIOLE ISL  FERIOLE ISLA FERIOLE ISLAN ERIOLE ISLAND RIOLE '
                  'ISLAND  IOLE ISLAND P OLE ISLAND PA LE ISLAND PAR E ISLAND '
                  'PARK SAINT FERIOLE  AINT FERIOLE I INT FERIOLE IS NT FERIOLE '
                  'ISL T FERIOLE ISLA  FERIOLE ISLAN FERIOLE ISLAND ERIOLE '
                  'ISLAND  RIOLE ISLAND P IOLE ISLAND PA OLE ISLAND PAR LE '
                  'ISLAND PARK SAINT FERIOLE I AINT FERIOLE IS INT FERIOLE '
                  'ISL NT FERIOLE ISLA T FERIOLE ISLAN  FERIOLE ISLAND '
                  'FERIOLE ISLAND  ERIOLE ISLAND P RIOLE ISLAND PA IOLE '
                  'ISLAND PAR OLE ISLAND PARK SAINT FERIOLE IS AINT FERIOLE '
                  'ISL INT FERIOLE ISLA NT FERIOLE ISLAN T FERIOLE ISLAND  '
                  'FERIOLE ISLAND  FERIOLE ISLAND P ERIOLE ISLAND PA RIOLE '
                  'ISLAND PAR IOLE ISLAND PARK SAINT FERIOLE ISL AINT FERIOLE '
                  'ISLA INT FERIOLE ISLAN NT FERIOLE ISLAND T FERIOLE ISLAND   '
                  'FERIOLE ISLAND P FERIOLE ISLAND PA ERIOLE ISLAND PAR RIOLE '
                  'ISLAND PARK SAINT FERIOLE ISLA AINT FERIOLE ISLAN INT FERIOLE '
                  'ISLAND NT FERIOLE ISLAND  T FERIOLE ISLAND P  FERIOLE ISLAND '
                  'PA FERIOLE ISLAND PAR ERIOLE ISLAND PARK SAINT FERIOLE ISLAN '
                  'AINT FERIOLE ISLAND INT FERIOLE ISLAND  NT FERIOLE ISLAND P '
                  'T FERIOLE ISLAND PA  FERIOLE ISLAND PAR FERIOLE ISLAND PARK '
                  'SAINT FERIOLE ISLAND AINT FERIOLE ISLAND  INT FERIOLE ISLAND '
                  'P NT FERIOLE ISLAND PA T FERIOLE ISLAND PAR  FERIOLE ISLAND '
                  'PARK SAINT FERIOLE ISLAND  AINT FERIOLE ISLAND P INT FERIOLE '
                  'ISLAND PA NT FERIOLE ISLAND PAR T FERIOLE ISLAND PARK SAINT '
                  'FERIOLE ISLAND P AINT FERIOLE ISLAND PA INT FERIOLE ISLAND '
                  'PAR NT FERIOLE ISLAND PARK SAINT FERIOLE ISLAND PA AINT '
                  'FERIOLE ISLAND PAR INT FERIOLE ISLAND PARK SAINT FERIOLE '
                  'ISLAND PAR AINT FERIOLE ISLAND PARK SAINT FERIOLE ISLAND PARK',
        'service_summary': 'Ambulatory Health Center, Community Clinic',
        'STATE': 'WI',
        'ZIP': '53821'
    }


@pytest.fixture
def example_IRS_service_data():
    return [
        {
            '_id': ObjectId('5f137b06236870dae2f1f5e4'),
            'STREET': 'PO BOX 376',
            'CITY': 'PR DU CHIEN',
            'NAME': 'ST FERIOLE ISLAND PARK',
            'NGRAMS': 'ST FERI T FERIO  FERIOL FERIOLE ERIOLE  RIOLE I IOLE IS '
            'OLE ISL LE ISLA E ISLAN  ISLAND ISLAND  SLAND P LAND PA AND PAR ND PARK ST '
            'FERIO T FERIOL  FERIOLE FERIOLE  ERIOLE I RIOLE IS IOLE ISL OLE ISLA LE '
            'ISLAN E ISLAND  ISLAND  ISLAND P SLAND PA LAND PAR AND PARK ST FERIOL T '
            'FERIOLE  FERIOLE  FERIOLE I ERIOLE IS RIOLE ISL IOLE ISLA OLE ISLAN LE '
            'ISLAND E ISLAND   ISLAND P ISLAND PA SLAND PAR LAND PARK ST FERIOLE T '
            'FERIOLE   FERIOLE I FERIOLE IS ERIOLE ISL RIOLE ISLA IOLE ISLAN OLE ISLAND '
            'LE ISLAND  E ISLAND P  ISLAND PA ISLAND PAR SLAND PARK ST FERIOLE  T FERIOLE '
            'I  FERIOLE IS FERIOLE ISL ERIOLE ISLA RIOLE ISLAN IOLE ISLAND OLE ISLAND  LE '
            'ISLAND P E ISLAND PA  ISLAND PAR ISLAND PARK ST FERIOLE I T FERIOLE IS  '
            'FERIOLE ISL FERIOLE ISLA ERIOLE ISLAN RIOLE ISLAND IOLE ISLAND  OLE ISLAND '
            'P LE ISLAND PA E ISLAND PAR  ISLAND PARK ST FERIOLE IS T FERIOLE ISL  '
            'FERIOLE ISLA FERIOLE ISLAN ERIOLE ISLAND RIOLE ISLAND  IOLE ISLAND P OLE '
            'ISLAND PA LE ISLAND PAR E ISLAND PARK ST FERIOLE ISL T FERIOLE ISLA  '
            'FERIOLE ISLAN FERIOLE ISLAND ERIOLE ISLAND  RIOLE ISLAND P IOLE ISLAND '
            'PA OLE ISLAND PAR LE ISLAND PARK ST FERIOLE ISLA T FERIOLE ISLAN  FERIOLE '
            'ISLAND FERIOLE ISLAND  ERIOLE ISLAND P RIOLE ISLAND PA IOLE ISLAND PAR OLE '
            'ISLAND PARK ST FERIOLE ISLAN T FERIOLE ISLAND  FERIOLE ISLAND  FERIOLE ISLAND '
            'P ERIOLE ISLAND PA RIOLE ISLAND PAR IOLE ISLAND PARK ST FERIOLE ISLAND T '
            'FERIOLE ISLAND   FERIOLE ISLAND P FERIOLE ISLAND PA ERIOLE ISLAND PAR RIOLE '
            'ISLAND PARK ST FERIOLE ISLAND  T FERIOLE ISLAND P  FERIOLE ISLAND PA FERIOLE '
            'ISLAND PAR ERIOLE ISLAND PARK ST FERIOLE ISLAND P T FERIOLE ISLAND PA  FERIOLE '
            'ISLAND PAR FERIOLE ISLAND PARK ST FERIOLE ISLAND PA T FERIOLE ISLAND PAR  '
            'FERIOLE ISLAND PARK ST FERIOLE ISLAND PAR T FERIOLE ISLAND PARK ST FERIOLE '
            'ISLAND PARK',
            'service_summary': 'Ambulatory Health Center, Community Clinic',
            'STATE': 'WI',
            'ZIP': '53821'
        },
        {
            '_id': ObjectId('5f7a5ab32410168bf92a4cbc'),
            'EIN': 10934058,
            'NAME': 'PRAIRIE FARM RIDGELAND FOOD PANTRY',
            'NGRAMS': 'PRAIRIE RAIRIE  AIRIE F IRIE FA RIE FAR IE FARM E FARM   FARM R FARM '
                      'RI ARM RID RM RIDG M RIDGE  RIDGEL RIDGELA IDGELAN DGELAND GELAND  '
                      'ELAND F LAND FO AND FOO ND FOOD D FOOD   FOOD P FOOD PA OOD PAN OD '
                      'PANT D PANTR  PANTRY PRAIRIE  RAIRIE F AIRIE FA IRIE FAR RIE FARM '
                      'IE FARM  E FARM R  FARM RI FARM RID ARM RIDG RM RIDGE M RIDGEL  '
                      'RIDGELA RIDGELAN IDGELAND DGELAND  GELAND F ELAND FO LAND FOO AND '
                      'FOOD ND FOOD  D FOOD P  FOOD PA FOOD PAN OOD PANT OD PANTR D PANTRY '
                      'PRAIRIE F RAIRIE FA AIRIE FAR IRIE FARM RIE FARM  IE FARM R E FARM '
                      'RI  FARM RID FARM RIDG ARM RIDGE RM RIDGEL M RIDGELA  RIDGELAN '
                      'RIDGELAND IDGELAND  DGELAND F GELAND FO ELAND FOO LAND FOOD AND '
                      'FOOD  ND FOOD P D FOOD PA  FOOD PAN FOOD PANT OOD PANTR OD PANTRY '
                      'PRAIRIE FA RAIRIE FAR AIRIE FARM IRIE FARM  RIE FARM R IE FARM RI '
                      'E FARM RID  FARM RIDG FARM RIDGE ARM RIDGEL RM RIDGELA M RIDGELAN  '
                      'RIDGELAND RIDGELAND  IDGELAND F DGELAND FO GELAND FOO ELAND FOOD '
                      'LAND FOOD  AND FOOD P ND FOOD PA D FOOD PAN  FOOD PANT FOOD PANTR '
                      'OOD PANTRY PRAIRIE FAR RAIRIE FARM AIRIE FARM  IRIE FARM R RIE FARM '
                      'RI IE FARM RID E FARM RIDG  FARM RIDGE FARM RIDGEL ARM RIDGELA RM '
                      'RIDGELAN M RIDGELAND  RIDGELAND  RIDGELAND F IDGELAND FO DGELAND FOO '
                      'GELAND FOOD ELAND FOOD  LAND FOOD P AND FOOD PA ND FOOD PAN D FOOD '
                      'PANT  FOOD PANTR FOOD PANTRY PRAIRIE FARM RAIRIE FARM  AIRIE FARM R '
                      'IRIE FARM RI RIE FARM RID IE FARM RIDG E FARM RIDGE  FARM RIDGEL FARM '
                      'RIDGELA ARM RIDGELAN RM RIDGELAND M RIDGELAND   RIDGELAND F RIDGELAND '
                      'FO IDGELAND FOO DGELAND FOOD GELAND FOOD  ELAND FOOD P LAND FOOD PA '
                      'AND FOOD PAN ND FOOD PANT D FOOD PANTR  FOOD PANTRY PRAIRIE FARM  '
                      'RAIRIE FARM R AIRIE FARM RI IRIE FARM RID RIE FARM RIDG IE FARM RIDGE '
                      'E FARM RIDGEL  FARM RIDGELA FARM RIDGELAN ARM RIDGELAND RM RIDGELAND  '
                      'M RIDGELAND F  RIDGELAND FO RIDGELAND FOO IDGELAND FOOD DGELAND FOOD  '
                      'GELAND FOOD P ELAND FOOD PA LAND FOOD PAN AND FOOD PANT ND FOOD PANTR '
                      'D FOOD PANTRY PRAIRIE FARM R RAIRIE FARM RI AIRIE FARM RID IRIE FARM '
                      'RIDG RIE FARM RIDGE IE FARM RIDGEL E FARM RIDGELA  FARM RIDGELAN FARM '
                      'RIDGELAND ARM RIDGELAND  RM RIDGELAND F M RIDGELAND FO  RIDGELAND FOO '
                      'RIDGELAND FOOD IDGELAND FOOD  DGELAND FOOD P GELAND FOOD PA ELAND FOOD '
                      'PAN LAND FOOD PANT AND FOOD PANTR ND FOOD PANTRY PRAIRIE FARM RI '
                      'RAIRIE FARM RID AIRIE FARM RIDG IRIE FARM RIDGE RIE FARM RIDGEL IE '
                      'FARM RIDGELA E FARM RIDGELAN  FARM RIDGELAND FARM RIDGELAND  ARM '
                      'RIDGELAND F RM RIDGELAND FO M '
                      'RIDGELAND FOO  RIDGELAND FOOD RIDGELAND FOOD  '
                      'IDGELAND FOOD P DGELAND FOOD PA GELAND FOOD PAN ELAND FOOD PANT LAND '
                      'FOOD PANTR AND FOOD PANTRY PRAIRIE FARM RID RAIRIE FARM RIDG AIRIE '
                      'FARM RIDGE IRIE FARM RIDGEL RIE FARM RIDGELA IE FARM RIDGELAN E '
                      'FARM RIDGELAND  FARM RIDGELAND  FARM RIDGELAND F ARM RIDGELAND FO '
                      'RM RIDGELAND FOO M RIDGELAND FOOD  RIDGELAND FOOD  RIDGELAND FOOD '
                      'P IDGELAND FOOD PA DGELAND FOOD PAN GELAND FOOD PANT ELAND FOOD '
                      'PANTR LAND FOOD PANTRY PRAIRIE FARM RIDG RAIRIE FARM RIDGE AIRIE '
                      'FARM RIDGEL IRIE FARM RIDGELA RIE FARM RIDGELAN IE FARM RIDGELAND '
                      'E FARM RIDGELAND   FARM RIDGELAND F FARM RIDGELAND FO ARM '
                      'RIDGELAND FOO RM RIDGELAND FOOD M RIDGELAND FOOD   RIDGELAND FOOD '
                      'P RIDGELAND FOOD PA IDGELAND FOOD PAN DGELAND FOOD PANT GELAND FOOD '
                      'PANTR ELAND FOOD PANTRY PRAIRIE FARM RIDGE RAIRIE FARM RIDGEL '
                      'AIRIE FARM RIDGELA IRIE FARM RIDGELAN RIE FARM RIDGELAND IE FARM '
                      'RIDGELAND  E FARM RIDGELAND F  FARM RIDGELAND FO FARM RIDGELAND '
                      'FOO ARM RIDGELAND FOOD RM RIDGELAND FOOD  M RIDGELAND FOOD P  '
                      'RIDGELAND FOOD PA RIDGELAND FOOD PAN IDGELAND FOOD PANT DGELAND '
                      'FOOD PANTR GELAND FOOD PANTRY PRAIRIE FARM RIDGEL RAIRIE FARM '
                      'RIDGELA AIRIE FARM RIDGELAN IRIE FARM RIDGELAND RIE FARM '
                      'RIDGELAND  IE FARM RIDGELAND F E FARM RIDGELAND FO  FARM RIDGELAND '
                      'FOO FARM RIDGELAND FOOD ARM RIDGELAND FOOD  RM RIDGELAND FOOD P '
                      'M RIDGELAND FOOD PA  RIDGELAND FOOD PAN RIDGELAND FOOD PANT IDGELAND '
                      'FOOD PANTR DGELAND FOOD PANTRY PRAIRIE FARM RIDGELA RAIRIE FARM '
                      'RIDGELAN AIRIE FARM RIDGELAND IRIE FARM RIDGELAND  RIE FARM '
                      'RIDGELAND F IE FARM RIDGELAND FO E FARM RIDGELAND FOO  FARM '
                      'RIDGELAND FOOD FARM RIDGELAND FOOD  ARM RIDGELAND FOOD P RM '
                      'RIDGELAND FOOD PA M RIDGELAND FOOD PAN  RIDGELAND FOOD PANT '
                      'RIDGELAND FOOD PANTR IDGELAND FOOD PANTRY PRAIRIE FARM RIDGELAN '
                      'RAIRIE FARM RIDGELAND AIRIE FARM RIDGELAND  IRIE FARM RIDGELAND F '
                      'RIE FARM RIDGELAND FO IE FARM RIDGELAND FOO E FARM RIDGELAND FOOD  '
                      'FARM RIDGELAND FOOD  FARM RIDGELAND FOOD P ARM RIDGELAND FOOD PA '
                      'RM RIDGELAND FOOD PAN M RIDGELAND FOOD PANT  RIDGELAND FOOD PANTR '
                      'RIDGELAND FOOD PANTRY PRAIRIE FARM RIDGELAND RAIRIE FARM RIDGELAND  '
                      'AIRIE FARM RIDGELAND F IRIE FARM RIDGELAND FO RIE FARM RIDGELAND '
                      'FOO IE FARM RIDGELAND FOOD E FARM RIDGELAND FOOD   FARM RIDGELAND '
                      'FOOD P FARM RIDGELAND FOOD PA ARM RIDGELAND FOOD PAN RM RIDGELAND '
                      'FOOD PANT M RIDGELAND FOOD PANTR  RIDGELAND FOOD PANTRY PRAIRIE FARM '
                      'RIDGELAND  RAIRIE FARM RIDGELAND F AIRIE FARM RIDGELAND FO IRIE FARM '
                      'RIDGELAND FOO RIE FARM RIDGELAND FOOD IE FARM RIDGELAND FOOD  E FARM '
                      'RIDGELAND FOOD P  FARM RIDGELAND FOOD PA FARM RIDGELAND FOOD PAN ARM '
                      'RIDGELAND FOOD PANT RM RIDGELAND FOOD PANTR M RIDGELAND FOOD PANTRY '
                      'PRAIRIE FARM RIDGELAND F RAIRIE FARM RIDGELAND FO AIRIE FARM '
                      'RIDGELAND FOO IRIE FARM RIDGELAND FOOD RIE FARM RIDGELAND FOOD  '
                      'IE FARM RIDGELAND FOOD P E FARM RIDGELAND FOOD PA  FARM RIDGELAND '
                      'FOOD PAN FARM RIDGELAND FOOD PANT ARM RIDGELAND FOOD PANTR RM '
                      'RIDGELAND FOOD PANTRY PRAIRIE FARM RIDGELAND FO RAIRIE FARM '
                      'RIDGELAND FOO AIRIE FARM RIDGELAND FOOD IRIE FARM RIDGELAND '
                      'FOOD  RIE FARM RIDGELAND FOOD P IE FARM RIDGELAND FOOD PA E '
                      'FARM RIDGELAND FOOD PAN  FARM RIDGELAND FOOD PANT FARM RIDGELAND '
                      'FOOD PANTR ARM RIDGELAND FOOD PANTRY PRAIRIE FARM RIDGELAND FOO '
                      'RAIRIE FARM RIDGELAND FOOD AIRIE FARM RIDGELAND FOOD  IRIE FARM '
                      'RIDGELAND FOOD P RIE FARM RIDGELAND FOOD PA IE FARM RIDGELAND FOOD '
                      'PAN E FARM RIDGELAND FOOD PANT  FARM RIDGELAND FOOD PANTR FARM '
                      'RIDGELAND FOOD PANTRY PRAIRIE FARM RIDGELAND FOOD RAIRIE FARM '
                      'RIDGELAND FOOD  AIRIE FARM RIDGELAND FOOD P IRIE FARM RIDGELAND '
                      'FOOD PA RIE FARM RIDGELAND FOOD PAN IE FARM RIDGELAND FOOD PANT '
                      'E FARM RIDGELAND FOOD PANTR  FARM RIDGELAND FOOD PANTRY PRAIRIE '
                      'FARM RIDGELAND FOOD  RAIRIE FARM RIDGELAND FOOD P AIRIE FARM '
                      'RIDGELAND FOOD PA IRIE FARM RIDGELAND FOOD PAN RIE FARM RIDGELAND '
                      'FOOD PANT IE FARM RIDGELAND FOOD PANTR E FARM RIDGELAND FOOD PANTRY '
                      'PRAIRIE FARM RIDGELAND FOOD P RAIRIE FARM RIDGELAND FOOD PA AIRIE '
                      'FARM RIDGELAND FOOD PAN IRIE FARM RIDGELAND FOOD PANT RIE FARM '
                      'RIDGELAND FOOD PANTR IE FARM RIDGELAND FOOD PANTRY PRAIRIE FARM '
                      'RIDGELAND FOOD PA RAIRIE FARM RIDGELAND FOOD PAN AIRIE FARM RIDGELAND '
                      'FOOD PANT IRIE FARM RIDGELAND FOOD PANTR RIE FARM RIDGELAND FOOD '
                      'PANTRY PRAIRIE FARM RIDGELAND FOOD PAN RAIRIE FARM RIDGELAND FOOD '
                      'PANT AIRIE FARM RIDGELAND FOOD PANTR IRIE FARM RIDGELAND FOOD PANTRY '
                      'PRAIRIE FARM RIDGELAND FOOD PANT RAIRIE FARM RIDGELAND FOOD PANTR '
                      'AIRIE FARM RIDGELAND FOOD PANTRY PRAIRIE FARM RIDGELAND FOOD PANTR '
                      'RAIRIE FARM RIDGELAND FOOD PANTRY PRAIRIE FARM RIDGELAND FOOD PANTRY',
            'STREET': '405 BLUFF AVE N', 'CITY': 'PRAIRIE FARM',
            'STATE': 'WI',
            'ZIP': '54762',
            'NTEE_CD': 'K31',
            'service_summary': 'Food Banks & Pantries',
            'service_type': 'FOOD',
            'service_subtype': 'Food Pantry',
            'source': 'IRS'
        },
        {
            '_id': ObjectId('5f7a5ab32410168bf92a4ca1'),
            'EIN': 10729555,
            'NAME': 'FIRST DEFENSE LEGAL AID',
            'NGRAMS': 'FIRST D IRST DE RST DEF ST DEFE T DEFEN  DEFENS DEFENSE EFENSE  FENSE '
                      'L ENSE LE NSE LEG SE LEGA E LEGAL  LEGAL  LEGAL A EGAL AI GAL AID '
                      'FIRST DE IRST DEF RST DEFE ST DEFEN T DEFENS  DEFENSE DEFENSE  '
                      'EFENSE L FENSE LE ENSE LEG NSE LEGA SE LEGAL E LEGAL   LEGAL A '
                      'LEGAL AI EGAL AID FIRST DEF IRST DEFE RST DEFEN ST DEFENS T '
                      'DEFENSE  DEFENSE  DEFENSE L EFENSE LE FENSE LEG ENSE LEGA NSE '
                      'LEGAL SE LEGAL  E LEGAL A  LEGAL AI LEGAL AID FIRST DEFE IRST '
                      'DEFEN RST DEFENS ST DEFENSE T DEFENSE   DEFENSE L DEFENSE LE '
                      'EFENSE LEG FENSE LEGA ENSE LEGAL NSE LEGAL  SE LEGAL A E LEGAL '
                      'AI  LEGAL AID FIRST DEFEN IRST DEFENS RST DEFENSE ST DEFENSE  '
                      'T DEFENSE L  DEFENSE LE DEFENSE LEG EFENSE LEGA FENSE LEGAL '
                      'ENSE LEGAL  NSE LEGAL A SE LEGAL AI E LEGAL AID FIRST DEFENS '
                      'IRST DEFENSE RST DEFENSE  ST DEFENSE L T DEFENSE LE  DEFENSE '
                      'LEG DEFENSE LEGA EFENSE LEGAL FENSE LEGAL  ENSE LEGAL A NSE '
                      'LEGAL AI SE LEGAL AID FIRST DEFENSE IRST DEFENSE  RST DEFENSE '
                      'L ST DEFENSE LE T DEFENSE LEG  DEFENSE LEGA DEFENSE LEGAL '
                      'EFENSE LEGAL  FENSE LEGAL A ENSE LEGAL AI NSE LEGAL AID FIRST '
                      'DEFENSE  IRST DEFENSE L RST DEFENSE LE ST DEFENSE LEG T DEFENSE '
                      'LEGA  DEFENSE LEGAL DEFENSE LEGAL  EFENSE LEGAL A FENSE LEGAL AI '
                      'ENSE LEGAL AID FIRST DEFENSE L IRST DEFENSE LE RST DEFENSE LEG ST '
                      'DEFENSE LEGA T DEFENSE LEGAL  DEFENSE LEGAL  DEFENSE LEGAL A '
                      'EFENSE LEGAL AI FENSE LEGAL AID FIRST DEFENSE LE IRST DEFENSE '
                      'LEG RST DEFENSE LEGA ST DEFENSE LEGAL T DEFENSE LEGAL   DEFENSE '
                      'LEGAL A DEFENSE LEGAL AI EFENSE LEGAL AID FIRST DEFENSE LEG '
                      'IRST DEFENSE LEGA RST DEFENSE LEGAL ST DEFENSE LEGAL  T DEFENSE '
                      'LEGAL A  DEFENSE LEGAL AI DEFENSE LEGAL AID FIRST DEFENSE LEGA '
                      'IRST DEFENSE LEGAL RST DEFENSE LEGAL  ST DEFENSE LEGAL A T '
                      'DEFENSE LEGAL AI  DEFENSE LEGAL AID FIRST DEFENSE LEGAL IRST '
                      'DEFENSE LEGAL  RST DEFENSE LEGAL A ST DEFENSE LEGAL AI T '
                      'DEFENSE LEGAL AID FIRST DEFENSE LEGAL  IRST DEFENSE LEGAL A '
                      'RST DEFENSE LEGAL AI ST DEFENSE LEGAL AID FIRST DEFENSE LEGAL '
                      'A IRST DEFENSE LEGAL AI RST DEFENSE LEGAL AID FIRST DEFENSE '
                      'LEGAL AI IRST DEFENSE LEGAL AID FIRST DEFENSE LEGAL AID',
            'STREET': '1111 N WELLS ST STE 308A',
            'CITY': 'CHICAGO', 'STATE': 'IL',
            'ZIP': '60610', 'NTEE_CD': 'I80',
            'service_summary': 'Legal Services',
            'service_type': 'RESOURCES',
            'service_subtype': 'Legal Assistance',
            'source': 'IRS'
        }
    ]


@pytest.fixture
def mock_mongo_client():
    return mongomock.MongoClient()


def test_check_similarity():
    mock_new_service = 'Example new service'
    mock_existing_service = 'Example existing service'
    assert not check_similarity(mock_new_service, mock_existing_service)


def test_check_similarity_st_vs_saint():
    mock_new_service = 'st dominics legal defense fund'
    mock_existing_service = 'saint dominics legal defense fund'
    assert check_similarity(mock_new_service, mock_existing_service)


def test_make_ngrams(example_IRS_service_data):
    service_name = example_IRS_service_data[1]['NAME']
    service_ngrams = example_IRS_service_data[1]['NGRAMS']
    print()
    assert ' '.join(make_ngrams(service_name)) == service_ngrams


def test_distance_with_empty_string():
    with pytest.raises(ZeroDivisionError):
        distance('', '')


def test_distance():
    assert round(distance('trench', 'wrench'), 3) == 0.833


def test_mock_collection_instantiation(example_IRS_service_data, mock_mongo_client):
    mock_services = mock_mongo_client.db.mock_services
    for obj in example_IRS_service_data:
        mock_services.insert_one(obj)
    obj = mock_services.find_one({'_id': ObjectId('5f137b06236870dae2f1f5e4')})
    assert obj == example_IRS_service_data[0]


def test_insert_services(example_IRS_service_data, mock_mongo_client):
    insert_services(example_IRS_service_data, mock_mongo_client.shelter, 'tmpIRS')
    found_services = [obj for obj in mock_mongo_client.shelter.tmpIRS.find()]
    assert example_IRS_service_data == found_services


def test_fuzzy_match_with_st_saint_discrepancy(
    example_IRS_service_data,
    example_IRS_search_object_with_spelled_out_saint,
    mock_mongo_client
):
    insert_services(example_IRS_service_data, mock_mongo_client.shelter, 'tmpIRS')
    refresh_ngrams(mock_mongo_client.shelter, 'tmpIRS')
    name = example_IRS_search_object_with_spelled_out_saint['NAME']
    zip_code = example_IRS_search_object_with_spelled_out_saint['ZIP']
    with pytest.raises(NotImplementedError):
        locate_potential_duplicate(
            name, zip_code, mock_mongo_client.shelter, 'tmpIRS'
        )  # Ensure this throws error, b/c if not, we can stop using the real connection below


@pytest.mark.realclient
def test_fuzzy_match(
    example_IRS_service_data,
    example_IRS_search_object_with_spelled_out_saint,
    mock_config_object
):
    client = MongoClient(
        "mongodb+srv://" + os.environ.get('DBUSERNAME')
        + ":" + os.environ.get('PW')
        + "@shelter-rm3lc.azure.mongodb.net/shelter?retryWrites=true&w=majority"
    )['shelter']
    if 'pytest_fuzzy_test' in client.list_collection_names():
        client.drop_collection('pytest_fuzzy_test')
    client.create_collection('pytest_fuzzy_test')
    insert_services(example_IRS_service_data, client, 'pytest_fuzzy_test')
    refresh_ngrams(client, 'pytest_fuzzy_test')
    name = example_IRS_search_object_with_spelled_out_saint['NAME']
    zip_code = example_IRS_search_object_with_spelled_out_saint['ZIP']
    dc = locate_potential_duplicate(
        name, zip_code, client, 'pytest_fuzzy_test'
    )
    client.drop_collection('pytest_fuzzy_test')
    assert dc == 'ST FERIOLE ISLAND PARK'
