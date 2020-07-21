from pymongo import MongoClient
import pandas as pd


# Dictionary of NTEE codes
# TODO: Add all NTEE codes with service summaries
NTEE_CD = {
  "K31": "Food Pantry",
  "154": "Health Clinic"
}


def read_csv(ntee_cd):
  """
  Filter a CSV by NTEE code.

  In:
  ntee_cd (str): NTEE code.

  Out:
  (DataFrame): Filtered CSV as Pandas data frame.
  """
  data = pd.read_csv("./Mid-Atlantic and Great Lakes Areas.csv", usecols = ["NAME", "STREET", "CITY", "STATE", "ZIP", "NTEE_CD"])
  filter_data = data["NTEE_CD"] == ntee_cd
  req_data = data[filter_data]
  return req_data


def query_db(data, ntee_cd):
  """
  Add data to tmp storage if name and zip already exists in services,
  otherwise add to services.

  In:
  data (DataFrame): Pandas data frame.
  ntee_cd (str): NTEE code.
  """
  client = MongoClient("mongodb+srv://{Username}:{Password}@shelter-rm3lc.azure.mongodb.net/test?authSource=admin&replicaSet=Shelter-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true")
  db = client["shelter"]
  services = db["services"]
  tmp = db["tmp"]

  for idx in data.index:
    service = services.find_one({
      "name": data["NAME"][idx],
      "zip": {
        "$regex": fr"{data['ZIP'][idx][:5]}"
      }
    })
    insert = {
      "name": data["NAME"][idx],
      "address1": data["STREET"][idx],
      "city": data["CITY"][idx],
      "state": data["STATE"][idx],
      "zip": data["ZIP"][idx][:5],
      "serviceSummary": NTEE_CD[ntee_cd]
    }

    if service:
      tmp.insert_one(insert)
    else:
      services.insert_one(insert)

if __name__ == "__main__":
  ntee_cd = "K31"
  data = read_csv(ntee_cd)
  query_db(data, ntee_cd)
