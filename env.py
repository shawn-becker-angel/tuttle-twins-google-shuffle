import os

# use pip install python-dotenv
from dotenv import load_dotenv

# loads the envars defined in the local .env file 
# so they can be accessed using os.getenv(envar, [default_value])
load_dotenv()

# season and episdoe manifest files are stored in S3 under s3://S3_MEDIA_ANGEL_NFT_BUCKET/S3_MANIFESTS_DIR
S3_MEDIA_ANGEL_NFT_BUCKET = os.getenv("S3_MEDIA_ANGEL_NFT_BUCKET")
S3_MANIFESTS_DIR = os.getenv("S3_MANIFESTS_DIR")

# season and episdoe manifest files are stored locally under LOCAL_MANIFESTS_DIR
# TO BE DELETED
LOCAL_MANIFESTS_DIR = os.getenv("LOCAL_MANIFESTS_DIR")

# This JSON file is required for Google Drive API functions.
# This file is created manually by members of the Angel Studios 
# Data team.
# See the README.md file for instructions
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")

# Directory for all types of episode and season data files
# e.g. 
# S01train.csv,  S01_test.csv,  S01_pred.csv
# S01E01_train.csv,  S01E01_test.csv,  S01E01_pred.csv
# S01E02_train.csv,  S01E02_test.csv,  S01E02_pred.csv
DATA_FILES_DIR = os.getenv("DATA_FILES_DIR")