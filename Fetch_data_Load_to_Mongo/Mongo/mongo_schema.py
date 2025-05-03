import pymongo

def create_collection_with_validation(mongo_client, DB_NAME, COL_NAME):
    try:
        client = mongo_client
        newdb = client[DB_NAME]
        
        # Define validation schema
        validation_schema = {
                            "$jsonSchema": {
                                "bsonType": "object",
                                "required": ["datetime", "symbol", "currency", "timezone", "open", "close", "high", "low", "volume"],
                                "properties": {
                                "datetime": {
                                    "bsonType": "date",
                                    "description": "must be a valid datetime"
                                },
                                "symbol": {
                                    "bsonType": "string",
                                    "description": "must be a non-empty string"
                                },
                                "currency": {
                                    "bsonType": "string",
                                    "description": "must be a non-empty string"
                                },
                                "timezone": {
                                    "bsonType": "string",
                                    "description": "must be a non-empty string"
                                },
                                "open": {
                                    "bsonType": "decimal",
                                    "description": "must be a Decimal128 value"
                                },
                                "high": {
                                    "bsonType": "decimal",
                                    "description": "must be a Decimal128 value"
                                },
                                "low": {
                                    "bsonType": "decimal",
                                    "description": "must be a Decimal128 value"
                                },
                                "close": {
                                    "bsonType": "decimal",
                                    "description": "must be a Decimal128 value"
                                },
                                "volume": {
                                    "bsonType": "int",
                                    "description": "must be an integer"
                                }
                                }
                            }
                            }

        
        # Create a collection with validation
        newdb.create_collection(COL_NAME, validator=validation_schema)
        print(f"Collection {COL_NAME} created with schema validation.")

    except pymongo.errors.ConnectionFailure as ce:
        print(f"Error connecting to MongoDB: {ce}")
    except pymongo.errors.PyMongoError as e:
        print(f"MongoDB error: {e}")
    except Exception as ex:
        print(f"An unexpected error occurred: {ex}")
    
