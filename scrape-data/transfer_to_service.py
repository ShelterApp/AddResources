import json
from service_defaults import service_default_values
from pymongo import MongoClient

# set the DB user name and password in config
with open('./config.json', 'r') as con:
    config = json.load(con)
# set in the config the path to schema translation json
with open("./" + config['schemaFile'], 'r') as sch:
    schema = json.load(sch)

service_to_origin_schema = schema
client = MongoClient(
        "mongodb+srv://"+config['userId']+":"+config['password']+"@shelter-rm3lc.azure.mongodb.net/shelter?retryWrites=true&w=majority")
db = client['shelter']
origin_db = db[config['originDb']]  # db with the documents to import
service = db[config['serviceDb']]  # db to add the new services

# function for getting document values given key of service table
# args: 
#   document (dict): This is a MongoDB document from the origin table 
#   key (string): this is a key from service schema
# yield:
#   returns (string) the value from origin document
def get_import_value(document, key):
    service_import_key = service_to_origin_schema[key]
    if type(service_import_key) is dict:
        # get the values from document, then filter out empty string
        values = filter(bool,
                        map(lambda key: document[key],
                            service_import_key["Keys"]))
        # concat import values from different keys
        return service_import_key["Delimiter"].join(values).strip()
    else:
        return document[service_import_key]

# function for translating document from origin table to match service
# args: 
#   document (dict): This is a MongoDB document from the origin table 
# yield:
#   returns (dict) MongoDB document that matches service table schema
def translate_document(document):
    # convert entries in import table to match service
    service = service_default_values.copy()
    for key in service_to_origin_schema.keys():
        import_value = get_import_value(document, key)
        service[key] = import_value
    # add imported service to new service array
    return service

# function for transfer all documents in a table to service table schema
# yield:
#   returns (array) list of MongoDB document that matches service table schema
def get_services():
    new_services = []
    for document in origin_db.find():
        new_services.append(translate_document(document))
    return new_services

# bulk post new services to service collection
service.insert_many(get_services())
