import pandas as pd
import numpy as np
import re
import gspread
import os
import time
import json
import random
from random import choices
import datetime
from manifest_row import ManifestRow

import typing
from typing import Any, List, Dict

from constants import S3_MEDIA_ANGEL_NFT_BUCKET, S3_MANIFESTS_DIR, LOCAL_MANIFESTS_DIR

# use pip install python-dotenv
from dotenv import load_dotenv
load_dotenv()

# This JSON file is required for Google Drive API functions.
# This file is created manually by members of the Angel Studios Data team.
# See the README.md file for instructions
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")

# Create the local manifest file and store it locally in LOCAL_MANIFIESTS_DIR
#
# Find and download the latest version of the matching manifest file in S3 
# 
# Compare the local file to the latest from s3
# if the local file differs from the latest s3 file
# synchronize the differences and upload the local manifest file to 
# s3://media.angel-nft.com/tuttle_twins/manifests/

def shuffle_manifest_rows(df: pd.DataFrame) -> pd.DataFrame:
    '''
    given a dataframe with N rows and these 2 columns, for example:
    "img_url": "https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/stamps/TT_S01_E01_FRM-00-00-08-20.jpg" 
    "img_class": "Common"

    return a dataframe with N rows and these 2 columns, for example:
    "src_url": "https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/stamps/TT_S01_E01_FRM-00-00-08-20.jpg" 
    "dst_key": "tuttle_twins/s01e01/ML/<dst_folder>/Common/TT_S01_E01_FRM-00-00-08-20.jpg"

    where <dst_folder> is a random dst_folder chosen from among 3 dst_folders with probablities:
    dst_folder_percentages = [("train", 0.70),("validate", 0.20),("test", 0.10)]
    '''
    dst_folder_percentages = [
        ("train", 0.7),
        ("validate", 0.2),
        ("test", 0.1)
    ]
    dst_folders = [x[0] for x in dst_folder_percentages]
    dst_percentages = [x[1] for x in dst_folder_percentages]
    df['dst_folder'] = random.choices(dst_folders, weights=dst_percentages, k=len(df))

    df['src_url'] = df['img_url']
    df['season_code'] = df['src_url'].str.extract(r"tuttle_twins/(s\d\d).*") 
    df['episode_code'] = df['src_url'].str.extract(r".*(e\d\d)/default_eng")
    df['img_file'] = df['src_url'].str.extract(r"stamps/(TT.*jpg)")
    df['dst_key'] = "tuttle_twins/" + df['season_code'] + df['episode_code'] + "/ML/" + df['dst_folder'] + "/" + df['img_class'] + "/" + df['img_file']

    df = df[['src_url', 'dst_key']]
    return df

def count_lines(filename) :
    with open(filename, 'r') as fp:
        num_lines = sum(1 for line in fp)
    return num_lines

def create_unversioned_episode_manifest_rows(episode_obj: dict) -> str:
    '''
    Read all rows of a "google episode sheet" described in the given
    "episode_obj" into a pandas dataframe called "episode_df".
    Compute and keep only columns 'img_url' and 'img_class'.
    Save the "episode_df" as an unversioned "episode_manifest file"
    in the local MANIFESTS_DIR. Each row of the "episode manifest file" 
    is formatted as a json string of a ManifestRow dict. 
    '''
    # get attributes from episode object
    season_code = episode_obj["season_code"].upper()
    episode_code = episode_obj["episode_code"].upper()
    share_link = episode_obj["share_link"]
    spreadsheet_url = episode_obj["spreadsheet_url"]

    unversioed_episode_manifest_file = f"{season_code}{episode_code}-manifest.jl"

    # use the google credentials file and the episode's share_link to read
    # the raw contents of the first sheet into episode_df
    gc = gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)
    gsheet = gc.open_by_url(share_link)
    data = gsheet.sheet1.get_all_records()
    episode_df = pd.DataFrame(data)

    num_rows = episode_df.shape[0]
    # df.info(verbose=True)
    # df.describe(include='all')
    print(f"input spread_sheet_url:{spreadsheet_url} num_rows:{num_rows}")

    # fetch the public 's3_thumbnails_base_url' from the name of column zero, e.g.
    #   https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/
    s3_thumbnails_base_url = episode_df.columns[0]

    # verify that s3_thumbnails_base_url contains 'episode_base_code', e.g. 
    #   "s01e01", which is constructed from episode properties "season_code" and "episode_code"
    episode_base_code = episode_obj["season_code"].lower() + episode_obj["episode_code"].lower()
    if s3_thumbnails_base_url.find(episode_base_code) == -1:
        raise Exception(f"s3_thumbnails_base_url fails to include 'episode_base_code': {episode_base_code}")

    # convert the s3_thumbnails_base_url into the s3_stamps_base_url
    s3_stamps_base_url = s3_thumbnails_base_url.replace("thumbnails","stamps")  

    # verify that all rows of the "FRAME NUMBER" column contain the 'episode_frame_code', e.g. 
    #   "TT_S01_E01_FRM"  
    # example FRAME_NUMBER column: 
    #   "TT_S01_E01_FRM-00-00-08-11"
    episode_frame_code = "TT_" + episode_obj["season_code"].upper() + "_" + episode_obj["episode_code"].upper() + "_FRM"
    matches = df[df['FRAME NUMBER'].str.contains(episode_frame_code, case=False)]
    failure_count = len(df) - len(matches)
    if failure_count > 0:
        raise Exception(f"{failure_count} rows have FRAME NUMBER values that don't contain 'episode_frame_code': {episode_frame_code}" )

    # compute the "img_url" column of each row using the s3_stamps_base_url and the "FRAME_NUMBER" of that row
    episode_df['img_url'] = s3_stamps_base_url + episode_df["FRAME NUMBER"] + ".jpg"

    # compute the "img_class" column of each row as the first available "CLASSIFICATION" for that row or None
    episode_df['img_class'] = \
        np.where(depisode_df["JONNY's RECLASSIFICATION"].str.len() > 0, episode_df["JONNY's RECLASSIFICATION"],
        np.where(episode_df["SUPERVISED CLASSIFICATION"].str.len() > 0, episode_df["SUPERVISED CLASSIFICATION"],
        np.where(episode_df["UNSUPERVISED CLASSIFICATION"].str.len() > 0, episode_df["UNSUPERVISED CLASSIFICATION"], None)))

    # drop all columns except these 
    episode_df = episode_df[['img_url','img_class']]

    # convert episode_df to a list of ManifestRow dicts
    unversioned_episode_manifest_rows = episode_df.to_dict('records')

    return unversioned_episode_manifest_rows


def compare_episode_manifests_rows(
    season_code: str, 
    episode_code: str, 
    local_rows: List[ManifestRow], 
    remote_rows: List[ManifestRow]):
    '''
    given local and remote list of manifest rows
    create a list of manifest action rows needed to bring
    the remote rows in sync with the local rows
    '''

def create_change_manifest(manifest, all_remote_files):
    change_manifest = []
    for item in manifest:
        change_manifest.append(file=item.file, old_folder=item.folder, new_folder=random_folder)
    return change_manifest

def create_copy_manifest(change_manifest)
    copy_manifest = []
    for change_item in change_manifest:
        if change_item.old_remote_folder is null and change_item.new_remote_folder is not null:
            copy_manifest.append(file=change_item.file, folder=ichange_item.new_remote_folder
    return copy_manifest
             
def create_delete_manifest(change_manifest)
    delete_manifest = []
    for change_item in change_manifest:
        if change_item.old_remote_folder is not null and change_item.new_remote_folder is null:
            delete_manifest.append(file=change_item.file, folder=change_item.old_remote_folder
    return delete_manifest

def apply_copy_manifest(copy_manifest):
    for copy_item in copy_manifest:
        copy(copy_item.file, copy_item.folder)

def apply_delete_manifest(delete_manifest):
    for delete_item in delete_manifest:
        delete(delete_item.file, delete_item.folder)
             

    episode_df = shuffle_manifest_rows(episode_df)


    # write all rows of manifest_jl_file to a json lines file under local_manifests_dir
    if not os.path.exists(LOCAL_MANIFESTS_DIR):
        os.makedirs(LOCAL_MANIFESTS_DIR)

    manifest_path = f"{LOCAL_MANIFESTS_DIR}/{manifest_jl_file}"
    # write each row_dist to the manifest_jl_file as a flat row_json_str
    with open(manifest_path, "w") as w: 
        for row_dict in df_list_of_row_dicts:
            row_json_str = json.dumps(row_dict) + "\n"
            # row_json_str = row_json_str.replace("\\/","/")
            w.write(row_json_str)
    
    num_lines = count_lines(manifest_path)
    print(f"output episode manifest_path:{manifest_path} num_lines:{num_lines}")


def parse_episode_manifest_version(episode_manifest_key: str) -> str:
    '''
    Parse out the utc_timestamp_iso portion of the given episode_manifest_key
    as the episode_manifest_version

        Parameters
        -----------
            episode_manifest_key (str)

        Returns
        ---------
            episode_manifest_version (str) or None if any exceptions were caught

        Notes
        -------
            Example episode manifest key:
            /tuttle_twins/manifests/S01E01-manifest-2022-05-02T12:43:24.662714.jl
            
            Example episode manifest version - a utc_datetime_iso string:
            2022-05-02T12:43:24.662714
    '''
    utc_datetime_iso_pattern = r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{6})"
    result = re.search(utc_datetime_iso_pattern, episode_manifest_key)
    version = result.group(1)
    return version


def download_all_s3_episode_objs() -> List[Dict]:
    '''
    Download the contents of all "season_manifest file"s 
    found in s3. Each "season_manifest file" contains a list
    of "episode dict"s. Return the concatonated list of all 
    "episode dicts" found from all "season manifest file"s.

        Parameters
        ----------
            None
        
        Returns
        -------
            An unorderd list of all "episode dict"s gathered from
            all "season manifest file"s. 

            An example "episode dict" (for season 1, episode 2):
            {
                "season_code": "S01",
                "episode_code": "E02",
                "spreadsheet_title": "Tuttle Twins S01E02 Unsupervised Clustering",
                "spreadsheet_url": "https://docs.google.com/spreadsheets/d/1v40TwUEphfX174xbAE-L3ORKqRz7S_jKeSeilibnkqQ/edit#gid=1690818184",
                "share_link": "https://docs.google.com/spreadsheets/d/1v40TwUEphfX174xbAE-L3ORKqRz7S_jKeSeilibnkqQ/edit?usp=sharing"
            }

            An example list of all "episode dicts" gathered from all "season_manifest file"s:
                [
                    {
                        "season_code": "S01",
                        "episode_code": "E01",
                        ...
                    },
                    {
                        "season_code": "S01",
                        "episode_code": "E02",
                        ...
                    },
                    {
                        "season_code": "S03",
                        "episode_code": "E11",
                        ...
                    },
                    ... other episodes in other seasons
                ]
       
        Notes
        -------
            The name of the "season_manifest file" that holds all "episode_obj"s for season 1:
            S01-episodes.json

            Example aws cli command to find all season manifest files:
            aws s3 ls s3://media.angel-nft.com/tuttle_twins/manifests/ | egrep "S\d\d-episodes.json"

    '''
    bucket = S3_MEDIA_ANGEL_NFT_BUCKET
    dir = "tuttle_twins/manifests"
    prefix = "S"
    suffix = "-episodes.json"

    season_manifest_keys = s3_list_files(bucket=bucket, dir=dir, prefix=prefix, suffix=suffix, verbose=False)
    if season_manifest_keys is None or len(season_manifest_keys) == 0:
        logger.info("zero season manifest files found in s3 - returning empty list")
        return []

    all_episode_objs = []
    for season_manifest_key in season_manifest_keys:
        key = season_manifest_key['key']
        try:
            tmp_file = "/tmp/season-manifest-" + datetime.datetime.utcnow().isoformat()
            s3_download_text_file(S3_MEDIA_ANGEL_NFT_BUCKET, key, tmp_file)
            episode_objs = json.load(tmp_file)
            all_episode_objs.append(episode_objs)
        finally:
            os.remove(tmp_file)

    return all_episode_objs

def s3_download_episode_manifest_file(bucket: str, key: str) -> List[ManifestRow]:
    '''
    download the contents of a single episode manifest file
        Parameters
        ----------
            bucket (str)
            key (str)
        
        Returns
        -------
            An unordered list of ManifestRow describing the contents of the given 
            episode manifest file given its bucket and key 
            
            Return an empty list if no manifest file is found.
        
        Notes
        -------
            Example episode manifest key:
            /tuttle_twins/manifests/S01E01-manifest-2022-05-02T12:43:24.662714.jl
        '''   
    # create a temp file to hold the contents of the download
    tmp_file = "/tmp/tmp-" + datetime.datetime.utcnow().isoformat()

    manifest_rows = []
    try:
        # use bucket and key to download manifest json lines file to local tmp_file 
        s3_download_text_file(manifest_bucket, manifest_key, tmp_file)

        # The manifest file is a text file with a json string on each line.
        # Decode each json_str into a json_dict and use ManifestRow to 
        # verify its structure. Then append the manifest_row to list of all manifest_rows. 
        with open(tmp_file, "r") as f:
            for json_str in f:
                json_dict = json.loads(json_str)
                manifest_row = ManifestRow(json_dict)
                manifest_rows.append(manifest_row)
    finally:
        # delete the tmp_file whether exceptions were thrown or not
        if os.path.exists(tmp_file):
            os.remove(tmp_file)

    return manifest_rows


def download_latest_s3_episode_manifest_file(season_code: str, episode_code: str) -> List[ManifestRow]:
    '''
    List the keys of all episode manifest files for the given season and 
    episode in S3. Return an empty list if no keys were found.

    Parse the 'version' string as the ending <utc_datetime_iso> 
    portion of each key and add it as a new 'version' property of each key. 

    Sort the list of keys by 'version' descending from highest to lowest.
    The latest manifest_key is first in the sorted list.

    Download and return the contents of latest manifest file as a list of 
    ManifestRow dicts. 

    Parameters
    ----------
        season_code (str): e.g. S01 for season 1
        episode_code (str): e.g. E08 for episode 8
    
    Returns
    -------
        An unordered list of ManifestRow describing the contents of the most recent episode manifest file 
        for the given season and episode in S3. Return an empty list if no manifest file is found.
    
    Notes
    -------
        Example s3 manifest filename:
        S01E01-manifest-2022-05-03T22:53:30.325223.jl

        Example aws cli command to find all episode manifest files for season 1 episode 1:
        aws s3 ls s3://media.angel-nft.com/tuttle_twins/manifests/S01E01-manifest- | egrep ".*\.jl"

        Example json string of an episode manifest file, with ManifestLine format:
        '{"src_url": "https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/stamps/TT_S01_E01_FRM-00-00-09-04.jpg", "dst_key": "tuttle_twins/s01e01/ML/validate/Legendary/TT_S01_E01_FRM-00-00-09-04.jpg"}'
    '''
    bucket = S3_MEDIA_ANGEL_NFT_BUCKET
    dir = "tuttle_twins/manifests"
    prefix = se_code =  (season_code + episode_code).upper()
    suffix = ".jl"

    manifest_keys = s3_list_files(bucket=bucket, dir=dir, prefix=prefix, suffix=suffix, verbose=False)
    if manifest_keys is None or len(manifest_keys) == 0:
        logger.info(f"zero episode manifest files found for {se_code} in s3 - returning empty list")
        return []

    # Parse the 'version' string as the ending <utc_datetime_iso> 
    # portion of each key and add it as a new 'version' property of each key. 
    for manifest_key in manifest_keys:
        version = parse_episode_manifest_version(manifest_key)
        manifest_key['version'] = version
    
    # sort the manifest_keys array by the last_modified attribute, descending from latest to earliest
    # the zero element is the latest key
    latest_manifest_key = sorted(manifest_keys, key=lambda x: x['version'], reverse=True)[0]

    # download the contents of the manifest file as a list of ManifestRow dicts
    manifest_rows = s3_download_manifest_file(S3_MEDIA_ANGEL_NFT_BUCKET, latest_manifest_key)
    return manifest_rows


def create_episode_manifest_files():
    '''
    Each "season_manifest_file", e.g. "S01-episodes.json" in s3 
    is a JSON file that contains a list of "episode_objs". Each
    "episode dict" is used to create a versioned "episode_manifest file" 

    These season_manifest_files are JSON files that are managed 
    manually by members of the Angel Studios Data team. 
    '''

    all_episode_objs = download_all_s3_episode_objs

    # create an episode manifest file for each episode dict
    for episode_obj in all_episode_objs:
        create_versioned_episode_manifest_file(episode_obj)

# =============================================
# TESTS
# =============================================

def test_parse_episode_manifest_version():
    episode_management_key = "s3://media.angel-nft.com/tuttle_twins/manifests/S01E01-manifest-2022-05-02T12:43:24.662714.jl"
    expected = "2022-05-02T12:43:24.662714"
    result = parse_episode_manifest_version(episode_management_key)
    assert result == expected, f"unexpected result:{result}"


if __name__ == "__main__":
    test_parse_episode_manifest_version()

