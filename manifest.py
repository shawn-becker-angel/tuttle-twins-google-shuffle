import os
import typing
from typing import TypedDict

# use pip install python-dotenv
from dotenv import load_dotenv

load_dotenv()

# Manifest files are stored under the S3 URI s3://S3_MEDIA_ANGEL_NFT_BUCKET/S3_MANIFESTS_DIR
S3_MEDIA_ANGEL_NFT_BUCKET = os.getenv("S3_MEDIA_ANGEL_NFT_BUCKET")
S3_MANIFESTS_DIR = os.getenv("S3_MANIFESTS_DIR")

# manifest.jl files are stored under LOCAL_MANIFESTS_DIR
LOCAL_MANIFESTS_DIR = os.getenv("LOCAL_MANIFESTS_DIR")

# season episode json files are under LOCAL_SEASON_EPISODES_DIR
LOCAL_SEASON_EPISODES_DIR = os.getenv("LOCAL_SEASON_EPISODES_DIR")

S3_PROCESS_BATCH_SIZE = int(os.getenv("S3_PROCESS_BATCH_SIZE", default=100))

 
class ManifestRow(TypedDict):
   src_url: str
   dst_key: str
