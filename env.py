import os

# use pip install python-dotenv
from dotenv import load_dotenv

# loads the envars defined in the local .env file 
# so they can be accessed using os.getenv(envar, [default_value])
load_dotenv()

# This JSON file is required for Google Drive API functions.
# This file is created manually by members of the Angel Studios 
# Data team.
# See the README.md file for instructions
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")
assert os.path.isfile(GOOGLE_CREDENTIALS_FILE)

# The main S3 bucket
S3_MEDIA_ANGEL_NFT_BUCKET = os.getenv("S3_MEDIA_ANGEL_NFT_BUCKET")

# Season manifest files are stored in S3 under s3://S3_MEDIA_ANGEL_NFT_BUCKET/S3_MANIFESTS_DIR
# e.g. S01-episodes.json
S3_MANIFESTS_DIR = os.getenv("S3_MANIFESTS_DIR")

# Local directory for the stage data files
# e.g. train.csv,  test.csv,  pred.csv
LOCAL_DATA_FILES_DIR = os.getenv("LOCAL_DATA_FILES_DIR")
assert os.path.isdir(LOCAL_DATA_FILES_DIR)

# Directory for local copies of all source image files synced from S3
LOCAL_SOURCE_IMAGES_DIR = os.getenv("LOCAL_SOURCE_IMAGES_DIR")
assert os.path.isdir(LOCAL_SOURCE_IMAGES_DIR)