import logging
from classes.APIClient import APIClient  
from classes.storageLib import ObjectStorageClient
from classes.generalLib import generalLib  
from classes.Silver2Gold import Silver2Gold  
from classes.Bronze2Silver import Bronze2Silver  
import pandas as pd


class Process:
    
    def api2bronze(self, api_url, bucket, endpoint_url, access_key, secret_key):
        try:
            brewery_api = APIClient(base_url=api_url, per_page=100)
            data = pd.DataFrame(brewery_api.fetch_paginated(endpoint="breweries"))
            
            client = ObjectStorageClient(endpoint_url=endpoint_url, access_key=access_key, secret_key=secret_key)
            client.upload_parquet(bucket, "data/bronze.parquet", data)
            
        except Exception as e:
            logging.error(f"Failed to read from API: {e}")
            raise
    
    def bronze2silver(self, api_url, bucket, endpoint_url, access_key, secret_key):
        try:
            client = ObjectStorageClient(endpoint_url=endpoint_url, access_key=access_key, secret_key=secret_key)
            bronze_df = pd.DataFrame(client.read_storage("bronze", "data/bronze.parquet"))
            logging.info(bronze_df.count())
            process = Bronze2Silver.bronze2silver(df=bronze_df)
            client.upload_parquet(bucket, "data/silver.parquet", process)
            logging.info(f"Silver Layer was successful")
        except Exception as e:
            logging.error(f"Failed Process Bronze to Silver: {e}")
            raise
    
    def silver2gold(self, api_url, bucket, endpoint_url, access_key, secret_key):
        try:
            client = ObjectStorageClient(endpoint_url=endpoint_url, access_key=access_key, secret_key=secret_key)
            silver_df = pd.DataFrame(client.read_storage("silver", "data/silver.parquet"))
            process = Silver2Gold.silver2gold(df=silver_df)
            client.upload_parquet(bucket, "data/gold.parquet", process)
            logging.info(f"Gold Layer was successful")
        except Exception as e:
            logging.error(f"Failed Process Silver to Gold: {e}")
            raise
            
    def validate_and_test(self, bucket, endpoint_url, access_key, secret_key):
        try:
            client = ObjectStorageClient(endpoint_url=endpoint_url, access_key=access_key, secret_key=secret_key)

            bronze_df = client.read_storage("bronze", "data/bronze.parquet")
            silver_df = client.read_storage("silver", "data/silver.parquet")
            gold_df = client.read_storage("gold", "data/gold.parquet")

            def summary(df, name):
                logging.info(f"{name} Layer - Rows: {len(df)}, Columns: {list(df.columns)}")
                logging.info(f"{name} Layer - Nulls:\n{df.isnull().sum()}")
                logging.info(f"{name} Layer - Dtypes:\n{df.dtypes}")
                logging.info(f"{name} Layer - Sample:\n{df.head(3)}")

            summary(bronze_df, "Bronze")
            summary(silver_df, "Silver")
            summary(gold_df, "Gold")

            # Example Test: Ensure row count flows down
            assert len(bronze_df) >= len(silver_df), "Silver should have <= Bronze rows"
            assert len(silver_df) >= len(gold_df), "Gold should have <= Silver rows"

            # Example Test: Column existence
            required_gold_cols = [
                "brewery_type", 
                "city", 
                "state_province", 
                "country"]
            for col in required_gold_cols:
                assert col in gold_df.columns, f"{col} not found in Gold layer"

            logging.info("All validation checks passed.")

        except Exception as e:
            logging.error(f"Validation failed: {e}")
            raise

    

    