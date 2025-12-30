import boto3
from botocore.config import Config as BotoConfig
from .config import Config

class R2Client:
    def __init__(self):
        self.client = boto3.client(
            's3',
            endpoint_url=Config.R2_ENDPOINT_URL,
            aws_access_key_id=Config.R2_ACCESS_KEY_ID,
            aws_secret_access_key=Config.R2_SECRET_ACCESS_KEY,
            config=BotoConfig(signature_version='s3v4')
        )
        self.bucket = Config.R2_BUCKET_NAME

    def list_files(self):
        response = self.client.list_objects_v2(Bucket=self.bucket)
        return response.get('Contents', [])

    def download_file(self, key, local_path):
        print(f"Downloading {key}...")
        self.client.download_file(self.bucket, key, local_path)
