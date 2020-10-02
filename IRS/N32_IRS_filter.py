import json
import pandas as pd
from pymongo import MongoClient

# set the DB user name and password in config
with open('config.json', 'r') as con:
  config = json.load(con)

def insert_mongoDb(data_to_insert):
  client = MongoClient(
      "mongodb+srv://democracylab:DemocracyLab2019@shelter-rm3lc.azure.mongodb.net/shelter?retryWrites=true&w=majority")
  db = client['shelter']
  db_coll = db['tmpIrsN32']

  db_coll.insert_many(data_to_insert.head().to_dict('records'))
  print("Successfully inserted " + str(data_to_insert.size) + "rows to mongodb.")


def remove_duplicates(filtered_data):
  client = MongoClient(
      "mongodb+srv://" + config['userId'] + ":" + config['password'] + "@shelter-rm3lc.azure.mongodb.net/shelter?retryWrites=true&w=majority")
  db = client['shelter']
  db_coll = db['services']

  # query mongodb if services collection already contains named service by zipcode
  col_names = ['name', 'address1', 'city', 'state', 'zip', 'service_summary']
  result = pd.DataFrame(columns=col_names)

  row_counter = 0
  for index, row in filtered_data.iterrows():
    service_name = row['NAME']
    zip = row['ZIP'].split('-')[0]
    found = db_coll.find_one("{$and: [{name:{$eq:"+service_name+"}},{zip:{$eq:"+zip+"}}]}")
    if not found:
      print("Found a data to insert with service: "+service_name+" and zip: "+zip)
      result.loc[row_counter] = [row['NAME'], row['STREET'], row['CITY'], row['STATE'], zip, "Ambulatory Health Center, Community Clinic"]
      row_counter += 1

  return result

if __name__ == "__main__":
  print("Reading the input csv file.")
  data = pd.read_csv('~/Downloads/Mid-Atlantic and Great Lakes Areas.csv', usecols=['EIN', 'NAME', 'STREET', 'CITY', 'STATE', 'ZIP', 'NTEE_CD'])

  # todo: also filter out : AA, AE, AP, MH, PW - we might need to ignore the following State name
  filter_exp = data['NTEE_CD'] == 'N32'
  req_data = data[filter_exp]

  if req_data.size > 0:
    print("found " + str(req_data.size) + " rows matching the NTEE code.")
    data_to_insert = remove_duplicates(req_data)

    if data_to_insert.size > 0:
      # print(data_to_insert.head())
      insert_mongoDb(data_to_insert)
