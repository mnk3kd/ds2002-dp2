from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)
# specify a database
db = client.mnk3kd
# specify a collection
db.dp2.drop()
collection = db.dp2

#data import portion
path = "data"

for (root, dirs, file) in os.walk(path):
    for f in file:
        print(f)


import json
#data is a directory, not a file. all files are under data directory
directory = "data"
files_imported = 0


for filename in os.listdir(directory):
    filepath = os.path.join(directory, filename)
    if os.path.isfile(filepath) and filename.endswith(".json"):
        # Load JSON data from the file
        try:
            with open(filepath) as file:
                file_data = json.load(file)

            # Insert the loaded data into the collection
            # if JSON contains data more than one entry,
            # insert_many is used, else insert_one is used
            if isinstance(file_data, list):
                try:
                    collection.insert_many(file_data)
                    print("inserted many")
                except Exception as e:
                    print(f"Error importing {filename}: {e}")
            else:
                try:
                    collection.insert_one(file_data)
                    print("inserted one")
                except Exception as e:
                    print(f"Error importing {filename}: {e}")
        except json.decoder.JSONDecodeError as e:
            print(f"Error decoding JSON in {filename}: {e}")
            # Log the error to a file if needed
            continue  # Skip to the next iteration of the loop
        except Exception as e:
            print(f"Error reading {filename}: {e}")
            # Log the error to a file if needed

print(collection.count_documents({}))
