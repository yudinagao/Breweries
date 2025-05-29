import logging
from classes.storageLib import ObjectStorageClient
from classes.generalLib import generalLib  


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

class Silver2Gold:
    
    @staticmethod
    def silver2gold(df):

        general= generalLib()        
        
        gold_df = df[
            ["brewery_type", 
            "city", 
            "state_province", 
            "country"]].value_counts()

        gold_df = gold_df.reset_index(
            name="brewery_count"
            ).sort_values(
                "brewery_count", ascending=False
                )
            
        gold_df = general.define_schema(gold_df, 
                                layer = "gold")
        
        
        return gold_df
    
    