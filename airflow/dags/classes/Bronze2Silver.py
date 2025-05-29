import logging
from classes.storageLib import ObjectStorageClient
from classes.generalLib import generalLib  
from datetime import datetime
import pandas as pd


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class Bronze2Silver:
    
    @staticmethod
    def bronze2silver(df):

        general= generalLib()           
        
        silver_df = general.define_schema(df
                                .drop_duplicates("id")
                                .assign(created_at=lambda x: datetime.now()), 
                                layer = "silver")
        
        return silver_df
    