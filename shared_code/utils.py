import logging
import os
from collections import OrderedDict

from pymongo import MongoClient, TEXT
from tqdm import tqdm
import re

# Establish global variables

client = MongoClient(
    "mongodb+srv://" + os.environ['DBUSERNAME'] + ":" + os.environ['PW']
#    + "@shelter-rm3lc.azure.mongodb.net/shelter?retryWrites=true&w=majority"
    + "@cluster0.dlr3b.mongodb.net/Cluster0?retryWrites=true&w=majority"
)['shelter']


def insert_services(data, client, collection):
    """Intake scraped services that have been processed and dupe-checked, and add to MongoDB.

    Args:
        data (dict): dictionary of IRS services containing ID, name, location and NTEE code
        client (obj): MongoClient object
        collection (str): the Mongo collection in which the data should be inserted
    """
    db = client
    db_coll = db[collection]
    if len(data) > 0:
        db_coll.insert_many(data)


def check_similarity(new_service, existing_service):
    regex = r'(st\.? |saint | inc\.?| nfp)'
    new_subbed_service = re.sub(regex, '', new_service).lower()
    existing_subbed_service = re.sub(regex, '', existing_service).lower()
    return distance(new_subbed_service, existing_subbed_service) >= 0.9


def make_ngrams(name, min_size=7):
    """Convert service name into list of n-grams.

    Args:
        name (str): the name of the service, e.g. 'FRIENDS OF LAKE HOPE'
        min_size (int, optional): the minimum number of characters in one ngram. Defaults to 7.

    Returns:
        list: list of ngram strings.
    """
    length = len(name)
    size_range = range(min_size, max(length, min_size) + 1)
    return list(OrderedDict.fromkeys(
        name[i:i + size]
        for size in size_range
        for i in range(0, max(0, length - size) + 1)
    ))


def refresh_ngrams(client, collection):
    """Make sure all the services in the desired collection have an ngram field.
       Also ensures that the n-gram field is included
       in the text index for the purpose of searching.

    Args:
        client (obj): pymongo MongoClient object
        collection (str): name of the collection in the db
    """
    coll = client[collection]
    for document in tqdm(coll.find()):
        try:
            coll.update_one(
                {"_id": document["_id"]},
                {"$set": {
                    "ngrams": ' '.join(make_ngrams(document["name"].upper()))
                }
                }
            )
        except KeyError:
            coll.update_one(
                {"_id": document["_id"]},
                {"$set": {
                    "ngrams": ' '.join(make_ngrams(document["NAME"].upper()))
                }
                }
            )
    # Check that ngram field is indexed
    if 'ngrams_text' not in coll.index_information().keys():
        coll.create_index([("ngrams", TEXT)])


def distance(a, b):
    """ Calculates the Levenshtein distance between a and b.
    """
    n, m = len(a), len(b)
    if n > m:
        # Make sure n <= m, to use O(min(n,m)) space
        a, b = b, a
        n, m = m, n

    current = range(n + 1)
    for i in range(1, m + 1):
        previous, current = current, [i] + [0] * n
        for j in range(1, n + 1):
            add, delete = previous[j] + 1, current[j - 1] + 1
            change = previous[j - 1]
            if a[j - 1] != b[i - 1]:
                change = change + 1
            current[j] = min(add, delete, change)

    return 1 - (current[n] / len(a))


def locate_potential_duplicate(name, zipcode, client, collection):
    """Search the desired db collection for services that might be
       fuzzy dupes of the service you're looking to add.

    Args:
        name (str): name of the service you want to add
        zipcode (str): string of the zip code of the service you want to add
        client (obj): pymongo MongoClient object
        collection (str): name of the db collection

    Returns:
        str: name of the service that might be a duplicate
    """
    grammed_name = make_ngrams(name)
    coll = client[collection]
    dupe_candidate = coll.find_one(
        {"$text": {"$search": ' '.join(grammed_name)}, 'zip': zipcode}
    )
    if dupe_candidate is not None:
        return dupe_candidate["name"]
    return False
