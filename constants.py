import os

# use pip install python-dotenv
from dotenv import load_dotenv

load_dotenv()

# Manifest files are stored under the S3 URI s3://S3_MEDIA_ANGEL_NFT_BUCKET/S3_MANIFESTS_DIR
S3_MEDIA_ANGEL_NFT_BUCKET = os.getenv("S3_MEDIA_ANGEL_NFT_BUCKET")
S3_MANIFESTS_DIR = os.getenv("S3_MANIFESTS_DIR")

# manifest.jl files are stored under LOCAL_MANIFESTS_DIR
LOCAL_MANIFESTS_DIR = os.getenv("LOCAL_MANIFESTS_DIR")

S3_PROCESS_BATCH_SIZE = int(os.getenv("S3_PROCESS_BATCH_SIZE", default=100))

