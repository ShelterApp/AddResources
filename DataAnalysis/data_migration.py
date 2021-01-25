import pandas as pd
import numpy as np
import os
from shared_code.utils import client
from pymongo import MongoClient

# Delete all data in a collection
def clean_collection(client, dataCollection):
    if dataCollection in client.list_collections(nameOnly = True):
        collection = client[dataCollection['name']]
        collection.delete_many({})

# Load all data from a collection in ShelterApp's database
def read_source(client, dataCollection):
    cursor = client[dataCollection].find()
    collection = pd.DataFrame(list(cursor))
    columnsToDrop = ['_id', 'approvedAt', 'likes', 'isShowFlag', 'isShowDonate', 'isContact', 'user', 'userEmail',
                     'createdAt', 'updatedAt', 'notes', 'crawledAt', 'working', 'ngrams', 'checkedOn', 'isCrawled',
                     'isSelectedAll', 'last_scrapped', 'last_updated', 'last_scraped', 'lastAdded', 'vEmailSent',
                     'currentstart', 'lastSigned', 'deletedAt']
    for column in columnsToDrop:
        if column in collection.columns:
            collection.drop(column, axis='columns', inplace=True)
    return collection

# Store all data from a collection into Atlas
def store(client, dataCollection, df):
    if not df.empty:
        collection = client[dataCollection['name']]
        dfDict = df.to_dict('records')
        df.fillna("-", inplace=True)
        collection.insert_many(dfDict)

dbRead = "mongodb+srv://" + os.environ['DBUSERNAME'] + ":" + os.environ['PW'] \
         + "@shelter-rm3lc.azure.mongodb.net/shelter?retryWrites=true&w=majority"

dbWrite = "mongodb+srv://" + os.environ['DBUSERNAME2'] + ":" + os.environ['PW2'] + \
          "@cluster0.sriji.mongodb.net/Cluster0?retryWrites=true&w=majority"

sourceClient = MongoClient(dbRead)['shelter']
targetClient = MongoClient(dbWrite)['shelter']

# Read, clean, and store data for each collection
for col in client.list_collections(nameOnly = True):
    print(col)
    df = read_source(sourceClient, col['name'])
    clean_collection(targetClient, col)
    store(targetClient, col, df)
