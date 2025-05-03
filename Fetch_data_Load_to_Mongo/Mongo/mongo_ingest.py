import pymongo
from dotenv import load_dotenv
import os
from Mongo.mongo_schema import create_collection_with_validation

load_dotenv(dotenv_path="Mongo\mongodb-atlas-config.env")

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
COL_NAME = os.getenv("COL_NAME")


def mongo_drop_and_create_collection():
    try:
        client = pymongo.MongoClient(MONGO_URI)
        newdb = client[DB_NAME]
        col = newdb[COL_NAME]
        col.drop() # Drop the collection , as we dont want old data in staging layer
        create_collection_with_validation(client, DB_NAME, COL_NAME)
    except pymongo.errors.ConnectionError as ce:
        print(f"Error connecting to MongoDB: {ce}")
    except pymongo.errors.PyMongoError as e:
        print(f"MongoDB error: {e}")
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")
    finally:
        client.close()   


def mongo_load_data(data):
    try:
        # Establish MongoDB connection
        client = pymongo.MongoClient(MONGO_URI)
        newdb = client[DB_NAME]
        col = newdb[COL_NAME]
        
        # Insert data into MongoDB collection
        if data:  # Check if data is not empty
            col.insert_many(data)
        else:
            print("No data to insert.")
    except pymongo.errors.ConnectionFailure as ce:
        print(f"Error connecting to MongoDB: {ce}")
    except pymongo.errors.PyMongoError as e:
        print(f"MongoDB error: {e}")
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")
    else:
        col.create_index([("datetime", pymongo.ASCENDING)]) # create index on datetime field    
    finally:
        client.close() 