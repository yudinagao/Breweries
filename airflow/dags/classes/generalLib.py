import pandas as pd
import logging


class generalLib:
    @staticmethod
    def define_schema (df, layer: str):
        if (layer == "silver"):
                schema = {
                    "id" : "string",
                    "name" : "string",
                    "brewery_type" : "string",
                    "address_1" : "string",
                    "address_2" : "string",
                    "address_3" : "string",
                    "city" : "string",
                    "state_province" : "string",
                    "postal_code" : "string",
                    "country" : "string",
                    "longitude" : "string",
                    "latitude" : "string",
                    "phone" : "string",
                    "website_url" : "string",
                    "state" : "string",
                    "street" : "string",
                    "created_at": "datetime64[ns]"
                    }
        elif (layer == "gold"):
                schema = {
                    "brewery_type" : "string",
                    "city" : "string",
                    "state_province" : "string",
                    "postal_code" : "string",
                    "country" : "string",
                    "brewery_count": "int64"
                }
        for col, dtype in schema.items():
            if col in df.columns:
                try:
                    df[col] = df[col].astype(dtype)
                except Exception as e:
                    logging.warning(f"Failed to cast column {col} to {dtype}: {e}")
        return df
            
        