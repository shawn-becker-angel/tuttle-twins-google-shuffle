import pandas as pd
import numpy as np
import re
import gspread
import os
import time
import json
from datetime import datetime

import typing
from typing import Any, List, Dict

from manifest import S3_MANIFEST_BUCKET, S3_MANIFESTS_DIR, LOCAL_MANIFESTS_DIR

# use pip install python-dotenv
from dotenv import load_dotenv
load_dotenv()

# This JSON file is required for Google Drive API functions.
# This file is created manually by members of the Angel Studios Data team.
# See the README.md file for instructions
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")

# Manifest files are stored under the S3 URI s3://S3_MANIFEST_BUCKET/S3_MANIFESTS_DIR
 = os.getenv("S3_MANIFEST_BUCKET")
S3_MANIFESTS_DIR = os.getenv("S3_MANIFESTS_DIR")

# Create the local manifest file and store it locally in LOCAL_MANIFIESTS_DIR
# Find and download the latest version of the matching manifest file in S3 
# 
# Compare the local file to the latest from s3
# if the local file differs from the latest s3 file
# synchronize the differences and upload the local manifest file to 
# s3://media.angel-nft.com/tuttle_twins/manifests/

def count_lines(filename) :
    with open(filename, 'r') as fp:
        num_lines = sum(1 for line in fp)
    return num_lines

def create_episode_manifest_jl_file(episode: List[Dict]):
    '''
    read the google sheet for this episode into a df
    compute and keep only columns 'img_url' and 'class'
    save this episode's df as a manifest.jl file 
    '''
    # get attributes from episode object
    share_link = episode["share_link"]
    manifest_jl_file = episode["manifest_jl_file"]
    spreadsheet_url = episode["spreadsheet_url"]

    # verify manifest file has '.jl' json lines extension
    if not manifest_jl_file.endswith(".jl"):
        raise Exception("episode manifest_jl_file:" + manifest_jl_file + " requires '.jl' json lines extension")

    # verify manifest file name has substring "<utc_datetime_iso>"
    if manifest_jl_file.find("<utc_datetime_iso>") == -1:
        raise Exception("episode manifest_jl_file:" + manifest_jl_file + " requires replaceable <utc_datetime_iso> substring")

    # replace <utc_datetime_iso> with the current value, e.g. '2022-04-28T10:43:48.733843'
    utc_datetime_iso = datetime.now().isoformat()
    manifest_jl_file = manifest_jl_file.replace("<utc_datetime_iso>", utc_datetime_iso)

    # use the google credentials file and the episode's share_link to read
    # the raw contents of the first sheet into df
    gc = gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)
    gsheet = gc.open_by_url(share_link)
    data = gsheet.sheet1.get_all_records()
    df = pd.DataFrame(data)

    num_rows = df.shape[0]
    # df.info(verbose=True)
    # df.describe(include='all')
    print(f"input spread_sheet_url:{spreadsheet_url} num_rows:{num_rows}")

    # fetch the public 's3_thumbnails_base_url' from the name of column zero, e.g.
    #   https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/
    s3_thumbnails_base_url = df.columns[0]

    # verify that s3_thumbnails_base_url contains 'episode_base_code', e.g. 
    #   "s01e01", which is constructed from episode properties "season_code" and "episode_code"
    episode_base_code = episode["season_code"].lower() + episode["episode_code"].lower()
    if s3_thumbnails_base_url.find(episode_base_code) == -1:
        raise Exception(f"s3_thumbnails_base_url fails to include 'episode_base_code': {episode_base_code}")

    # convert the s3_thumbnails_base_url into the s3_stamps_base_url
    s3_stamps_base_url = s3_thumbnails_base_url.replace("thumbnails","stamps")  

    # verify that all rows of the "FRAME NUMBER" column contain the 'episode_frame_code', e.g. 
    #   "TT_S01_E01_FRM"  
    # example FRAME_NUMBER column: 
    #   "TT_S01_E01_FRM-00-00-08-11"
    episode_frame_code = "TT_" + episode["season_code"].upper() + "_" + episode["episode_code"].upper() + "_FRM"
    matches = df[df['FRAME NUMBER'].str.contains(episode_frame_code, case=False)]
    failure_count = len(df) - len(matches)
    if failure_count > 0:
        raise Exception(f"{failure_count} rows have FRAME NUMBER values that don't contain 'episode_frame_code': {episode_frame_code}" )

    # compute the "img_url" column of each row using the s3_stamps_base_url and the "FRAME_NUMBER" of that row
    df['img_url'] = s3_stamps_base_url + df["FRAME NUMBER"] + ".jpg"

    # compute the "class_name" column of each row as the first available "CLASSIFICATION" for that row or None
    df['class_name'] = \
        np.where(df["JONNY's RECLASSIFICATION"].str.len() > 0, df["JONNY's RECLASSIFICATION"],
        np.where(df["SUPERVISED CLASSIFICATION"].str.len() > 0, df["SUPERVISED CLASSIFICATION"],
        np.where(df["UNSUPERVISED CLASSIFICATION"].str.len() > 0, df["UNSUPERVISED CLASSIFICATION"], None)))

    # drop all columns except these 
    df = df[['img_url','class_name']]

    # convert df to a list of dicts, one for each row
    df_list_of_row_dicts = df.to_dict('records')

    # write all rows of manifest_jl_file to a json lines file under local_manifests_dir
    if not os.path.exists(local_manifests_dir):
        os.makedirs(local_manifests_dir)

    manifest_path = f"{LOCAL_MANIFESTS_DIR}/{manifest_jl_file}"
    # write each row_dist to the manifest_jl_file as a flat row_json_str
    with open(manifest_path, "w") as w: 
        for row_dict in df_list_of_row_dicts:
            row_json_str = json.dumps(row_dict)
            # row_json_str = row_json_str.replace("\\/","/")
            w.writelines(row_json_str)
    
    num_lines = count_lines(manifest_path)
    print(f"output episode manifest_path:{manifest_path} num_lines:{num_lines}")


def create_season_manifest_jl_files():
    '''
    Each season_episode_file, e.g. "S01-episodes.json" describes the parameters 
    used to create episode_manifest files for all of its episodes.

    These JSON files are created manually by members of the Angel Studios Data team
    '''
    season_episode_files = [f for f in os.listdir(LOCAL_SEASON_EPISODES_DIR) if re.match(r'S\d\d-episodes.json', f)]

    # create an episode manifest file for all episodes in each season
    for season_episode_file in season_episode_files:
        with open(season_episode_file,"r") as f:
            season_episodes = json.load(f)
            for episode in season_episodes:
                create_episode_manifest_jl_file(episode)


if __name__ == "__main__":
    create_season_manifest_jl_files()


