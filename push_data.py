import os
import sys
from typing import List, Dict, Any
import pandas as pd
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, PyMongoError
from dotenv import load_dotenv
import certifi

# Custom modules
from networksecurity.exception.exception import NetworkSecurityException
from networksecurity.logging.logger import logging

load_dotenv()

class NetworkDataManager:
    """Manager for handling network data operations with MongoDB."""
    
    def __init__(self, mongo_uri: str = None):
        """
        Initialize MongoDB connection.
        
        Args:
            mongo_uri: MongoDB connection URI (optional)
        """
        self.mongo_uri = mongo_uri or os.getenv("MONGO_DB_URL")
        if not self.mongo_uri:
            raise NetworkSecurityException("MongoDB URI not found in environment variables", sys)
        
        self.ca = certifi.where()

    def csv_to_json_records(self, file_path: str) -> List[Dict[str, Any]]:
        """Convert CSV file to JSON-formatted records for MongoDB."""
        try:
            logging.info(f"Converting CSV file: {file_path}")
            df = pd.read_csv(file_path)
            return df.to_dict(orient='records')
        except (FileNotFoundError, pd.errors.EmptyDataError) as e:
            logging.error(f"CSV processing error: {str(e)}")
            raise NetworkSecurityException(e, sys) from e

    def insert_to_mongodb(
        self,
        records: List[Dict[str, Any]],
        database_name: str,
        collection_name: str
    ) -> int:
        """Insert records into MongoDB collection."""
        try:
            with MongoClient(
                self.mongo_uri,
                tlsCAFile=self.ca  # SSL certificate handling
            ) as client:
                db = client[database_name]
                collection = db[collection_name]
                
                result = collection.insert_many(records)
                logging.info(f"Inserted {len(result.inserted_ids)} records")
                return len(result.inserted_ids)
                
        except ConnectionFailure as e:
            logging.error("MongoDB connection failed")
            raise NetworkSecurityException(e, sys) from e
        except PyMongoError as e:
            logging.error(f"MongoDB operation failed: {str(e)}")
            raise NetworkSecurityException(e, sys) from e

if __name__ == "__main__":
    try:
        # Configuration
        CSV_PATH = "Data/networkData.csv"
        DB_NAME = "network_analytics"
        COLLECTION_NAME = "traffic_data"
        
        # Initialize manager
        manager = NetworkDataManager()
        
        # Process data
        records = manager.csv_to_json_records(CSV_PATH)
        inserted_count = manager.insert_to_mongodb(records, DB_NAME, COLLECTION_NAME)
        
        print(f"Successfully inserted {inserted_count} records")
        
    except NetworkSecurityException as e:
        logging.error(f"Network Security Operation Failed: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        sys.exit(1)