import json
from service_defaults import service_default_values
from pymongo import MongoClient

with open('./config.json', 'r') as con:
    config = json.load(con)
with open("./" + config['schemaFile'], 'r') as sch:
    schema = json.load(sch)

service_to_origin_db = schema
client = MongoClient(
        "mongodb+srv://"+config['userId']+":"+config['password']+"@shelter-rm3lc.azure.mongodb.net/shelter?retryWrites=true&w=majority")
db = client['shelter']
origin_db = db[config['originDb']]  # db with the entries to import
service = db[config['serviceDb']]  # db to add the new entries


def get_import_value(document, key):
    service_import_key = service_to_origin_db[key]
    if type(service_import_key) is dict:
        # first filter out empty string
        values = filter(bool,
                        map(lambda key: document[key],
                            service_import_key["Keys"]))
        # concat import values from different keys
        return service_import_key["Delimiter"].join(values).strip()
    else:
        return document[service_import_key]


def get_services():
    new_services = []
    for document in origin_db.find():
        # convert entries in import table to match service
        service = service_default_values.copy()
        for key in service_to_origin_db.keys():
            import_value = get_import_value(document, key)
            service[key] = import_value
        # add imported service to new service array
        print(service)
        new_services.append(service)
    return new_services

# bulk post new services to service collection
service.insert_many(get_services())
