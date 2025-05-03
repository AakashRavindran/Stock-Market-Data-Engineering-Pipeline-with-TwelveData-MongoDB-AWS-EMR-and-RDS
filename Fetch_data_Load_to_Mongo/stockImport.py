import requests
from credentials import API_KEY
from datetime import datetime
from bson import Decimal128
from Mongo.mongo_ingest import mongo_drop_and_create_collection,mongo_load_data


COMPANIES = {"Apple": "AAPL", "Microsoft": "MSFT", "Tesla": "TSLA", "Google" : "GOOGL", "Nvidia": "NVDA"}

CURRENCY = "USD"
TIMEZONE = "America/New_York"

def fetch_stock_data_and_load_to_mongo():
    mongo_drop_and_create_collection() # Drop the old data in staging layer of MongoDB


    for company in COMPANIES:
        params = {
            "symbol": COMPANIES.get(company),
            "interval": "5min",
            "outputsize": 1500,            
            "apikey": API_KEY,
            "format": "JSON"
        }

        try:
            # Make the API request
            response = requests.get("https://api.twelvedata.com/time_series", params=params)
            
            # Check if the response is successful (status code 200)
            if response.status_code == 200:
                data = response.json()

                # Check if 'values' key exists
                if "values" in data:
                    formatted_data = []
                    
                    for record in data["values"]:
                        # Convert the values into MongoDB compatible schema
                        record["symbol"] = COMPANIES.get(company)
                        record["currency"] = CURRENCY
                        record["timezone"] = TIMEZONE
                        record["open"] = Decimal128(record["open"])
                        record["high"] = Decimal128(record["high"])
                        record["low"] = Decimal128(record["low"])
                        record["close"] = Decimal128(record["close"])
                        record["volume"] = int(record["volume"])

                        # Convert each record's datetime to ISO format
                        try:
                            record["datetime"] = datetime.strptime(record["datetime"], "%Y-%m-%d %H:%M:%S")
                            formatted_data.append(record)
                        except ValueError as ve:
                            print(f"Skipping invalid datetime for {company}: {record['datetime']}")

                    # Insert data into MongoDB
                    if formatted_data:
                        print(f"Inserting data for {company} into MongoDB.")
                        mongo_load_data(formatted_data)
                    else:
                        print(f"No valid data to insert for {company}")

                else:
                    print(f"No 'values' found in response for {company}")

            else:
                print(f"Error fetching data for {company}: {response.status_code} - {response.text}")

        except requests.exceptions.RequestException as e:
            print(f"Request error for {company}: {e}")
        except ValueError as ve:
            print(f"JSON parsing error for {company}: {ve}")

if __name__ == "__main__":
    fetch_stock_data_and_load_to_mongo()
        
