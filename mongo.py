from pymongo import MongoClient
from datetime import datetime


class DbClient(MongoClient):
    
    accepted_schemas = {
        "TZDB_TIMEZONES": [
            "countrycode",
            "countryname",
            "zonename",
            "gmtoffset",
            "import_date"
        ],
        
        "TZDB_ZONE_DETAILS": [
            "countrycode",
            "countryname",
            "zonename",
            "gmtoffset",
            "dst",
            "zonestart",
            "zoneend",
            "import_date"
        ],
        "TZDB_ERROR_LOG": [
            "error_date",
            "error_message"
        ]
    }
    def __init__(self, url= "localhost", port = 27017) -> None:
        self.url = url
        self.port = port
        self.client = MongoClient(url, port)
        

    def get_db(self, collection):
        return self.client[collection]


    def insert(self, collection, data, check_exists=False):
        '''
        Inserts data into mongo based on collection name, allows for upserting.

        Params: 
        collection (str) -> Name of collection to access
        data [{}] (list of dicts) -> Data to be inserted into the collection
        
        
        '''
        # Access db and get into collection's "info" table
        db = self.get_db(collection)
        db_collection = db["info"]

        # iterate over list of dictionaries and operate on each
        for dictionary in data:
            # go through dictionary keys and pop any unaccepted fields based on the db schema
            for field in list(dictionary):
                if field.lower() not in self.accepted_schemas[collection]:
                    dictionary.pop(field)
            # Flag to query Database and determine if the entry already exists
            if check_exists:
                # If something is found, remove the data
                if db_collection.find(dictionary):
                    data.remove(dictionary)
                    continue
        
            # Add import_date
            dictionary["import_date"] = dictionary.get("formatted", datetime.now())
       
        # Insert the remaining data
        if data:
            db_collection.insert_many(data)

    

    def get(self, collection):
        '''
        Gets all records in a collection from DB

        Params:

        colleciton (str) -> name of the collection to access
        '''
        db = self.get_db(collection)
        db_collection = db["info"]
        return list(db_collection.find({}, {"_id":0}))

    def delete(self, collection):
        '''
        Deletes ALL records within a collection from DB

        Params:

        colleciton (str) -> name of the collection to access
        '''
        db = self.get_db(collection)
        db_collection = db["info"]
        db_collection.delete_many({})
        return 

