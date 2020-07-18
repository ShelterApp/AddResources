import json
import pandas as pd
from pymongo import MongoClient

def insert_mongoDb(data_to_insert):
  client = MongoClient(
      "mongodb+srv://democracylab:DemocracyLab2019@shelter-rm3lc.azure.mongodb.net/shelter?retryWrites=true&w=majority")

  print("Successfully inserted " + str(data_to_insert.size) + "rows to mongodb.")


def remove_duplicates(filtered_data):
  client = MongoClient(
      "mongodb+srv://"+config['userId']+":"+config['password']+"@shelter-rm3lc.azure.mongodb.net/shelter?retryWrites=true&w=majority")

  db = client['shelter']
  destinationDb = db['']
  return filtered_data

# set the DB user name and password in config
with open('./configIRS.json', 'r') as con:
  config = json.load(con)

if __name__ == "__main__":
  print("Reading the input csv file.")
  data = pd.read_csv('~/Downloads/PuertoRico.csv', usecols=['EIN', 'NAME', 'STREET', 'CITY', 'STATE', 'ZIP', 'NTEE_CD'])
  filter_exp = data['NTEE_CD'] == 'B20*'
  req_data = data[filter_exp]

  if req_data.size > 0:
    print("found " + str(req_data.size) + " rows matching the NTEE code.")
    data_to_insert = remove_duplicates(req_data)

    if data_to_insert.size > 0:
      insert_mongoDb(data_to_insert)
