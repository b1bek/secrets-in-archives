import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    R2_ENDPOINT_URL = os.getenv("R2_ENDPOINT_URL")
    R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
    R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
    R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
    DATABASE_URL = os.getenv("DATABASE_URL")
    TEMP_DIR = "temp_processing"

    @classmethod
    def validate(cls):
        required = [
            "R2_ENDPOINT_URL", 
            "R2_ACCESS_KEY_ID", 
            "R2_SECRET_ACCESS_KEY", 
            "R2_BUCKET_NAME",
            "DATABASE_URL"
        ]
        missing = [key for key in required if not getattr(cls, key)]
        if missing:
            raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
