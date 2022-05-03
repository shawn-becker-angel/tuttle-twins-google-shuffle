import os
import typing
from typing import TypedDict

# use pip install python-dotenv
from dotenv import load_dotenv

load_dotenv()

# Manifest files are stored under the S3 URI s3://S3_MANIFEST_BUCKET/S3_MANIFESTS_DIR
S3_MANIFEST_BUCKET = os.getenv("S3_MANIFEST_BUCKET")
S3_MANIFESTS_DIR = os.getenv("S3_MANIFESTS_DIR")

 
class ManifestRow(TypedDict):
   img_url: str
   class_name: str
