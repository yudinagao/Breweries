import boto3
import logging
from botocore.exceptions import ClientError
import pandas as pd
import io
import os

class ObjectStorageClient:
    def __init__(self, endpoint_url=None, access_key=None, secret_key=None):
        self.s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000" or os.getenv("S3_ENDPOINT_URL", "http://localhost:9000"),
            aws_access_key_id="minioadmin" or os.getenv("S3_ACCESS_KEY"),
            aws_secret_access_key="minio@1234!" or os.getenv("S3_SECRET_KEY"),
        )

    def ensure_bucket(self, bucket_name):
        try:
            self.s3.head_bucket(Bucket=bucket_name)
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])

            if error_code == 404:
                try:
                    self.s3.create_bucket(Bucket=bucket_name)
                except ClientError as create_err:
                    if create_err.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
                        pass
                    else:
                        raise
            else:
                raise


    def upload_parquet(self, bucket, key, dataframe: pd.DataFrame):
        self.ensure_bucket(bucket)

        buffer = io.BytesIO()
        dataframe.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        self.s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream"
        )
        logging.info(f"Parquet uploaded to: {bucket}/{key}")
    
    
    def read_storage(self, bucket, key):
        try:
            client = self.s3.get_object(Bucket=bucket, Key=key)
            data = client['Body'].read()
            return pd.read_parquet(io.BytesIO(data))
        except Exception as e:
            logging.error(f"Failed in reading {bucket} layer: {e}")
            raise

    