The script transfer_to_service takes documents from an origin table and then translates it to match the schema of the service table. 

Command to run:
```
 python3 transfer_to_service.py
```
Set up: 
1. You might need to install MongoDB client and DNS
```
python3 -m pip install pymongo
python3 -m pip install dnspython
```
2. Update `config.json` with the following
3. Add your username and password 
3. Set `originDb`, origin table you want to translate 
4. Set `serviceDb`, service table you want write the new entries
5. Set `schemaFile` to the path with json that translates the schemas

Structure of translation schema, example nw_hospitality_schema.json:
The json object is meant to translate keys from service table to corresponding keys in the origin table
Key: string key from service table
Value: string key from origin table that coorespond 

If the service key maps to multiple keys from the origin table. The values need to be concated. The following json object will be set as the Value.
Key: "Keys"
Value: Array of string key from the origin table that coorespond
Key: "Delimiter"
Value: string use to seperate the values from the origin table