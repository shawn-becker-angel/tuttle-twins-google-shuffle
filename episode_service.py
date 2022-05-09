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

from pkg_resources import get_default_cache
from manifest_row import ManifestRow
from episode import Episode
from manifest import Manifest
from s3_utils import s3_list_files, s3_delete_files, s3_copy_files

from season_service import download_all_season_episodes

import typing
from typing import Any, List, Dict

from constants import S3_MEDIA_ANGEL_NFT_BUCKET, S3_MANIFESTS_DIR, LOCAL_MANIFESTS_DIR

# use pip install python-dotenv
from dotenv import load_dotenv
load_dotenv()

# This JSON file is required for Google Drive API functions.
# This file is created manually by members of the Angel Studios 
# Data team.
# See the README.md file for instructions
GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE")

# ============================================
# episode_service overview
#
# episodes = season_service.load_all_season_episodes()

# --------------------------
# load all episodes from the episode JSON files stored within 
# the S3 manifests directory, set 'episode_id' as 
# 'season_code' + 'episode_code'
 
# G, S = find_google_episode_keys(episode)
# --------------------------
# use google sheet data for episode_id to define 
# G with columns [episode_id, img_src, img_frame, randomized new_folder, manual new_img_class]

# C = s3_find_episode_jpg_keys(episode)
# --------------------------
# finds current jpg keys under tuttle_twins/ML with episode_id
# C with columns [last_modified, size, key, folder, img_class, img_frame, season_code, episode_code, episode_id]
# C -> columns [episode_id, img_frame, folder, img_class]

#-------------------------
# J1 = C join G on [episode_id, img_frame] to create
# J1 with columns [episode_id, img_frame, non-nullable img_class, non-nullable folder, nullable new_img_class, nullable new_folder] 
# J1 -> columns=[episode_id, img_frame, key, new_key]
# J1 assert key is never null
# J1 discard rows where key == new_key
# J1 where new_key is null delete key file and drop key 
# J1 where key != new_key copy key file to new_key file, delete key file, drop key

#-----------------------------
# J2 = G join C on [episode_id, img_frame] to create
# J2 = columns=[episode_id, img_frame, new_img_class, new_folder, nullable img_class, nullable folder ] 
# J2 assert when img_class is null then folder is null
# J2 -> columns [episode_id, img_frame, new_key, key]
# J2 assert new key is never null
# J2 assert size new_key == key is zero

#-----------------------------
# J3 = J2 join S on [episode_id, img_frame] to create
# J3 with columns = [episode_id, img_frame, new_key, key, img_src]
# J3 where new_key is not null and key is null copy img_src file to new_key file

#-----------------------------
# C4 =  s3_find_episode_jpg_keys(episode)
# C4 with columns [last_modified, size, key, folder, img_class, img_frame, season_code, episode_code, episode_id]
# C4 -> columns [episode_id, img_frame, folder, img_class]

#-----------------------------
# G still has columns [episode_id, img_frame, randomized new_folder, manual new_img_class]
# G2 -> G with columns [episode_id, img_frame, folder, img_class]

#-----------------------------
# assert C4 == G2

def add_randomized_new_folder_column(df: pd.DataFrame) -> pd.DataFrame:
    '''
    per a percentage-wise distribution
    '''
    percentage_distribution = [
        ("train", 0.7),
        ("validate", 0.2),
        ("test", 0.1)
    ]
    choices = new_folders = [x[0] for x in percentage_distribution]
    weights = new_percentages = [x[1] for x in percentage_distribution]
    k = len(df)

    df['new_folder'] = random.choices(choices, weights=weights, k=k)
    return df


def find_google_episode_keys(episode: Episode) -> pd.DataFrame:
    '''
    Read all rows of a "google episode sheet" described in the given episode into 
    G with columns=[episode_id, img_src, img_frame, manual new_img_class]
    then add randomized new_folder column
    '''
    # get attributes from episode object
    episode_id = episode["episode_id"]
    share_link = episode["share_link"]

    # use the google credentials file and the episode's share_link to read
    # the raw contents of the first sheet into G
    gc = gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)
    gsheet = gc.open_by_url(share_link)
    data = gsheet.sheet1.get_all_records()
    df = pd.DataFrame(data)

    # fetch the public 's3_thumbnails_base_url' from the name of column zero, e.g.
    #   https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/
    s3_thumbnails_base_url = df.columns[0]

    # verify that s3_thumbnails_base_url contains episode_id.lower() e.g. s01e01
    episode_id_lower = episode_id.lower()
    if s3_thumbnails_base_url.find(episode_id_lower) == -1:
        raise Exception(f"s3_thumbnails_base_url fails to include {episode_id_lower}")

    # verify that all rows of the "FRAME NUMBER" column contain the 'episode_frame_code', e.g. 
    #   "TT_S01_E01_FRM"  
    # example FRAME_NUMBER column: 
    #   "TT_S01_E01_FRM-00-00-08-11"
    season_code, episode_code = Episode.parse_episode_id(episode_id)
    episode_frame_code = ("TT_" + season_code + "_" + episode_code + "_FRM").upper()
    matches = df[df['FRAME NUMBER'].str.contains(episode_frame_code, case=False)]
    failure_count = len(df) - len(matches)
    if failure_count > 0:
        raise Exception(f"{failure_count} rows have FRAME NUMBER values that don't contain 'episode_frame_code': {episode_frame_code}" )

    # save the episode_id
    df['episode_id'] = episode_id

    # compute the "img_url" column of each row using the s3_thumbnails_base_url and the "FRAME_NUMBER" of that row
    df['img_url'] = s3_thumbnails_base_url + df["FRAME NUMBER"] + ".jpg"

    # store the "FRAME_NUMBER" column as "img_frame"
    df['img_frame'] = df["FRAME NUMBER"]

    # compute the "new_class" column as the first available "CLASSIFICATION" for that row or None
    df['new_class'] = \
        np.where(df["JONNY's RECLASSIFICATION"].str.len() > 0, df["JONNY's RECLASSIFICATION"],
        np.where(df["SUPERVISED CLASSIFICATION"].str.len() > 0, df["SUPERVISED CLASSIFICATION"],
        np.where(df["UNSUPERVISED CLASSIFICATION"].str.len() > 0, df["UNSUPERVISED CLASSIFICATION"], None)))
    
    # add randomized column 'new_folder'
    df = add_randomized_new_folder_column(df)
    
    # keep only these columns
    df = df[['episode_id', 'img_url','img_frame', 'new_folder', 'new_class']]

    return df

def split_key_in_df(df: pd.DataFrame) -> pd.DataFrame:
    '''
    split df.key to add columns ['folder', 'img_class', 'img_frame', 'season_code', 'episode_code', 'episode_id']
    '''
    # e.g. df.key = "tuttle_twins/s01e01/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"

    key_cols = ['tt','se','ml','folder','img_class','img_frame','ext']
    key_df = df.key.str.split('/|\.', expand=True).rename(columns = lambda x: key_cols[x])
    key_df = key_df.drop(columns=['tt','se','ml','ext'])
    df = pd.concat([df,key_df], axis=1)

    # e.g. df.folder = "validate"
    # e.g. df.img_class = "Uncommon"
    # e.g. df.img_frame = "TT_S01_E01_FRM-00-19-16-19"
    
    img_frame_cols = ['tt', 'season_code', 'episode_code', 'remainder']
    img_frame_df = df.img_frame.str.split('_', expand=True).rename(columns = lambda x: img_frame_cols[x])
    img_frame_df = img_frame_df.drop(columns=['tt','remainder'])
    
    # e.g. df.season_code = "S01"
    # e.g. df.episode_code = "E01"
    
    img_frame_df['episode_id'] = img_frame_df.season_code + img_frame_df.episode_code
    # e.g. df.episode_id = "S01E01"

    df = pd.concat([df,img_frame_df], axis=1)   

    return df

def s3_find_episode_jpg_keys(episode: Episode) -> pd.DataFrame:
    '''
    find current jpg keys under tuttle_twins/ML with episode_id
    C = columns=[last_modified, size, key, folder, img_class, img_frame, season_code, episode_code, episode_id]
    C -> columns=[episode_id, img_frame, folder, img_class]
    '''
    bucket = S3_MEDIA_ANGEL_NFT_BUCKET
    dir = "tuttle_twins/ML"
    suffix = ".jpg"
    
    # example episode_id: S01E01, split_episode_id: S01_E01
    episode_id = episode['episode_id']
    split_episode_id = Episode.split_episode_id(episode_id)

    # example key: tuttle_twins/ML/validate/Rare/TT_S01_E01_FRM-00-00-09-01.jpg
    episode_key_pattern = f"TT_{split_episode_id}_FRM-.+\.jpg"
    
    episode_keys_list = s3_list_files(bucket=bucket, dir=dir, key_pattern=episode_key_pattern)

    # create dataframe with columns ['last_modified', 'size', 'key']
    df = pd.DataFrame(episode_keys_list, columns=['last_modified', 'size', 'key'])
    
    # split df.key to add columns ['folder', 'img_class', 'img_frame', 'season_code', 'episode_code', 'episode_id']
    df = split_key_in_df(df)
    
    assert df['episode_id'] == episode_id

    df = df[['episode_id', 'img_frame', 'folder', 'img_class']]
    
    return df


def main() -> None:
    all_episodes = download_all_season_episodes()
    for episode in all_episodes:
        G = find_google_episode_keys(episode)
        C = s3_find_episode_jpg_keys(episode)

        #-------------------------
        # J1 = C join G on [episode_id, img_frame] to create
        # J1 with columns [episode_id, img_frame, non-nullable img_class, non-nullable folder, nullable new_img_class, nullable new_folder] 
        # J1 -> columns=[episode_id, img_frame, key, new_key]
        # J1 discard rows where key == new_key
        # J1 where new_key is null delete key file 
        # J1 where key != new_key copy key file to new_key file, delete key file
        
        J1 = C.join(G,  
            how='inner', 
            on=['episode_id', 'img_frame'], 
            sort=False)
        expected = set(['episode_id', 'img_frame', 'img_class', 'folder', 'img_url', 'new_img_class', 'new_folder'])
        result = set(J1.columns())
        assert result == expected, f"ERROR: expected J1.columns: {expected} not {result}"
        
        # J1 -> columns=[episode_id, img_frame, key, new_key]
        J1['key'] = J1['folder'] + '/' + J1['img_class']
        J1['new_key'] = J1['new_folder'] + '/' + J1['new_img_class']
        J1 = J1[['episode_id', 'img_frame', 'key', 'new_key']]
        
        # J1 discard rows where key == new_key
        J1 = J1[J1['key'] != J1['new_key']]

        # J1 where new_key is null delete key file and drop key 
        J1_del = J1.dropna(subset=['new_key'])
        # tuttle_twins/ML/validate/Rare/TT_S01_E01_FRM-00-00-09-01.jpg
        J1_del['del_key'] = "tuttle_twins/ML/" + J1_del['key'] + '/' + J1_del['img_frame'] + ".jpg"
        del_keys = list(J1_del['del_key'].to_numpy())
        s3_delete_files(bucket=S3_MEDIA_ANGEL_NFT_BUCKET, keys=del_keys)

        # J1 where key != new_key copy key file to new_key file, delete key file
        J1_cp = J1[(J1['key'] is not None) and (J1['new_key'] is not None) and (J1['key'] != J1['new_key'])]
        J1_cp['src_key'] = "tuttle_twins/ML/" + J1_cp['key'] + '/' + J1_del['img_frame'] + ".jpg"
        J1_cp['dst_key'] = "tuttle_twins/ML/" + J1_cp['new_key'] + '/' + J1_del['img_frame'] + ".jpg"
        J1_cp = J1_cp[['src_key','dst_key']]
        src_keys = list(J1_cp['src_key'].to_numpy())
        dst_keys = list(J1_cp['dst_key'].to_numpy())
        s3_copy_files(src_bucket=S3_MEDIA_ANGEL_NFT_BUCKET, src_keys=src_keys, 
                      dst_bucket=S3_MEDIA_ANGEL_NFT_BUCKET, dst_keys=dst_keys)
        s3_delete_files(bucket=S3_MEDIA_ANGEL_NFT_BUCKET, keys=src_keys)


# def init_manifest_old_keys(episode: Episode, manifest_df: Manifest) -> Manifest:

#     # use an inner join of manifest_df with episode_keys_df to set the 
#     # 'old_key', 'old_folder' and 'old_class' columns

#     # manifest_df columns:
#     # ['episode_id','img_url','img_frame',
#     # 'old_key','old_class','old_folder',
#     # 'new_key','new_class','new_folder]
    
#     manifest_df = manifest_df[['episode_id','img_frame','new_class']]

#     # episode_keys_df columns: 
#     # ['last_modified', 'size', 'key', 'folder', 'img_class', 'img_frame']
#     episode_keys_df = find_episode_keys(episode)
#     episode_keys_df['old_class'] = episode_keys_df['img_class']
#     episode_keys_df['old_folder'] = episode_keys_df['folder']
#     # keep only these columns
#     episode_keys_df = episode_keys_df[['img_frame', 'old_class', 'old_folder']]

#     # expected columns of the inner join on img_frame
#     expected_set  = set(['episode_id','img_url','img_frame',
#     # 'old_key','old_class','old_folder',
#     # 'new_key','new_class','new_folder]
        
#         'img_url','img_frame','old_class','folder',, 'new_key','new__folder'])

#     joined_df = manifest_df.join(
#         episode_keys_df, 
#         how='inner', 
#         on=['img_frame','old_class'], 
#         lsuffix='old_', 
#         rsuffix='new_', 
#         sort=False)

#     result_set = set(joined_df.columns)
#     assert join_columns_set == expected, f"unexpected join results: {result_set} != {expected_set}"

#     manifest_df = joined_df

#     # set 'ml_folder' to 'r_ml_folder'
#     manifest_df['ml_folder'] = manifest_df['r_ml_folder']

#     # keep only the ManifestRow columns
#     manifest_df = manifest_df['img_url','img_frame','img_class','ml_folder', 'new_ml_folder']

#     return manifest_df


# def randomize_manifest_new_keys(episode: Episode, manifest_df: Manifest) -> Manifest:
#     '''
#     Set the new_folder column of teh given manifest_df 
#     to a random new_folder per a percentage-wise distribution
#     '''
#     new_ml_folder_percentages = [
#         ("train", 0.7),
#         ("validate", 0.2),
#         ("test", 0.1)
#     ]
#     choices = new_ml_folders = [x[0] for x in new_ml_folder_percentages]
#     weights = new_ml_percentages = [x[1] for x in new_ml_folder_percentages]
#     k = len(manifest_df)

#     manifest_df['new_ml_folder'] = random.choices(choices, weights=weights, k=k)
#     return manifest_df


# def count_lines(filename) :
#     with open(filename, 'r') as fp:
#         num_lines = sum(1 for line in fp)
#     return num_lines

# def get_all_ml_keys_df():

#     # set search criteria for all ML keys
#     bucket = S3_MEDIA_ANGEL_NFT_BUCKET
#     dir = "tuttle_twins/ML"
#     prefix = None
#     suffix = ".jpg"

#     all_ml_keys = s3_list_files(bucket=bucket, dir=dir, prefix=prefix, suffix=suffix)

#     # use list of s3_key dict to create dataframe with s3_key columns ["last_modified" , "size", "key"]
#     all_ml_keys_df = pd.DataFrame(all_ml_keys, columns=["last_modified" , "size", "key"])
#     return all_ml_keys_df

# def save_versioned_manifest_file(episode: Episode) -> str:
#     '''
#     Create a json lines file with the format of ManifestRow dict and save
#     it as an unversioned episode manifest file locally.

#     Arguments
#     ----------
#         episode: an Episode dict for an episode in a season

#     Returns
#     ----------
#         the name of the local unversioned manifest file
#     '''

#     # Use the given episode to create an manifest_df with undefined 
#     # 'new_folder' and 'new_ml_folder' columns from that episode's google shet
#     manifest_df = create_manifest_df_from_google_sheet(episode)

#     # Use init_ml_folder to initialize the 'ml_folder' from the current list of all ml_keys
#     init_old_ml_folder_column(manifest_df)

#     # compoute the path of the local unversioned manifest file
#     unversioned_manifest_file = f"{LOCAL_MANIFESTS_DIR}/{season_code}{episode_code}-manifest.jl"

#     # Write the df as json lines of format ManifestRow dict to the local JL file
#     manifest_df.to_json(unversioned_manifest_file, orient='records')
#     return unversioned_manifest_file


# def randomize_new_ml_folder(manifest_df: pd.DataFrame) -> pd.DataFrame:
#     '''
#     Set the new_ml_folder column of teh given manifest_df 
#     to a random new_folder per a percentage-wise distribution
#     '''
#     new_ml_folder_percentages = [
#         ("train", 0.7),
#         ("validate", 0.2),
#         ("test", 0.1)
#     ]
#     choices = new_ml_folders = [x[0] for x in new_ml_folder_percentages]
#     weights = new_ml_percentages = [x[1] for x in new_ml_folder_percentages]
#     k = len(manifest_df)

#     manifest_df['new_ml_folder'] = random.choices(choices, weights=weights, k=k)
#     return manifest_df

# def compare_manifests_rows(
#     season_code: str, 
#     episode_code: str, 
#     local_rows: List[ManifestRow], 
#     remote_rows: List[ManifestRow]):
#     '''
#     given local and remote list of manifest rows
#     create a list of manifest action rows needed to bring
#     the remote rows in sync with the local rows

#     '''





# def df_from_manifest_rows(manifest: List[ManifestRow]) -> pd.DataFrame:
#     '''
#     ManifestRow: dict {src_url: str, dst_key: str}
#     '''
#     manifest_df = pd.DataFrame(manifest,columns=['src_url', 'dst_key'])
#     manifest_df.shape
#     return manifest_df


# def create_change_manifest_df(manifest_df):
#     # manifest_df 
#     # image_frame
#     # image_class
#     # folder clurrent location
#     # new_folder future randomized 
#     dst_folder_percentages = [
#         ("train", 0.7),
#         ("validate", 0.2),
#         ("test", 0.1)
#     ]
#     dst_folders = [x[0] for x in dst_folder_percentages]
#     dst_percentages = [x[1] for x in dst_folder_percentages]

#     df = manifest_df.copy(deep=True)
#     # {"src_url": "https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/stamps/TT_S01_E01_FRM-00-00-08-19.jpg", "dst_key": "tuttle_twins/s01e01/ML/train/Common/TT_S01_E01_FRM-00-00-08-19.jpg"}

#     df['file'] = df['src_url].str.replace("")
#     change_manifest_df['dst_folder'] = random.choices(dst_folders, weights=dst_percentages, k=len(manifest_df))

#     change_manifest = []
#     for item in manifest:
#         change_manifest.append(file=item.file, old_folder=item.folder, new_folder=random_folder)
#     return change_manifest

# def create_copy_manifest(change_manifest):
#     copy_manifest = []
#     for change_item in change_manifest:
#         if change_item.old_remote_folder is null and change_item.new_remote_folder is not null:
#             copy_manifest.append(file=change_item.file, folder=ichange_item.new_remote_folder
#     return copy_manifest
             
# def create_delete_manifest(change_manifest):
#     delete_manifest = []
#     for change_item in change_manifest:
#         if change_item.old_remote_folder is not null and change_item.new_remote_folder is null:
#             delete_manifest.append(file=change_item.file, folder=change_item.old_remote_folder
#     return delete_manifest

# def apply_copy_manifest(copy_manifest):
#     for copy_item in copy_manifest:
#         copy(copy_item.file, copy_item.folder)

# def apply_delete_manifest(delete_manifest):
#     for delete_item in delete_manifest:
#         delete(delete_item.file, delete_item.folder)
             

#     manifest_df = shuffle_manifest_rows(manifest_df)


#     # write all rows of manifest_jl_file to a json lines file under local_manifests_dir
#     if not os.path.exists(LOCAL_MANIFESTS_DIR):
#         os.makedirs(LOCAL_MANIFESTS_DIR)

#     manifest_path = f"{LOCAL_MANIFESTS_DIR}/{manifest_jl_file}"
#     # write each row_dist to the manifest_jl_file as a flat row_json_str
#     with open(manifest_path, "w") as w: 
#         for row_dict in df_list_of_row_dicts:
#             row_json_str = json.dumps(row_dict) + "\n"
#             # row_json_str = row_json_str.replace("\\/","/")
#             w.write(row_json_str)
    
#     num_lines = count_lines(manifest_path)
#     print(f"output episode manifest_path:{manifest_path} num_lines:{num_lines}")


# def parse_manifest_version(manifest_key: str) -> str:
#     '''
#     Parse out the utc_timestamp_iso portion of the given manifest_key
#     as the manifest_version

#         Parameters
#         -----------
#             manifest_key (str)

#         Returns
#         ---------
#             manifest_version (str) or None if any exceptions were caught

#         Notes
#         -------
#             Example episode manifest key:
#             /tuttle_twins/manifests/S01E01-manifest-2022-05-02T12:43:24.662714.jl
            
#             Example episode manifest version - a utc_datetime_iso string:
#             2022-05-02T12:43:24.662714
#     '''
#     utc_datetime_iso_pattern = r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d{6})"
#     result = re.search(utc_datetime_iso_pattern, manifest_key)
#     version = result.group(1)
#     return version


# def download_all_s3_episode_objs() -> List[Dict]:
#     '''
#     Download the contents of all "season_manifest file"s 
#     found in s3. Each "season_manifest file" contains a list
#     of "episode dict"s. Return the concatonated list of all 
#     "episode dicts" found from all "season manifest file"s.

#         Parameters
#         ----------
#             None
        
#         Returns
#         -------
#             An unorderd list of all "Episode" dicts gathered from
#             all "season manifest file"s. 

#             An example list of all "Episode" dicts gathered from all "season_manifest file"s:
#                 [
#                     { # Episode dict for season 1 episode 1
#                         "season_code": "S01",
#                         "episode_code": "E01",
#                         ...
#                     },
#                     { # Episode dict for season 1 episode 2
#                         "season_code": "S01",
#                         "episode_code": "E02",
#                         ...
#                     },
#                     ...
#                     { # Episode dict for season 3 episode 11
#                         "season_code": "S03",
#                         "episode_code": "E11",
#                         ...
#                     },
#                     ... other Episode in other seasons
#                 ]
       
#         Notes
#         -------
#             The name of the "season_manifest file" that holds all Episode dicts for season 1:
#             S01-episodes.json

#             Example aws cli command to find all season manifest files:
#             aws s3 ls s3://media.angel-nft.com/tuttle_twins/manifests/ | egrep "S\d\d-episodes.json"

#     '''
#     bucket = S3_MEDIA_ANGEL_NFT_BUCKET
#     dir = "tuttle_twins/manifests"
#     prefix = "S"
#     suffix = "-episodes.json"

#     season_manifest_keys = s3_list_files(bucket=bucket, dir=dir, prefix=prefix, suffix=suffix, verbose=False)
#     if season_manifest_keys is None or len(season_manifest_keys) == 0:
#         logger.info("zero season manifest files found in s3 - returning empty list")
#         return []

#     all_episode_objs = []
#     for season_manifest_key in season_manifest_keys:
#         key = season_manifest_key['key']
#         try:
#             tmp_file = "/tmp/season-manifest-" + datetime.datetime.utcnow().isoformat()
#             s3_download_text_file(S3_MEDIA_ANGEL_NFT_BUCKET, key, tmp_file)
#             episode_objs = json.load(tmp_file) # List[Episode]
#             all_episode_objs.append(episode_objs)
#         finally:
#             os.remove(tmp_file)

#     return all_episode_objs

# def s3_download_manifest_file(bucket: str, key: str) -> List[ManifestRow]:
#     '''
#     download the contents of a single episode manifest file stored in s3
#         Parameters
#         ----------
#             bucket (str)
#             key (str)
        
#         Returns
#         -------
#             An unordered list of ManifestRow describing the contents of the given 
#             episode manifest file given its bucket and key 
            
#             Return an empty list if no manifest file is found.
        
#         Notes
#         -------
#             Example episode manifest key in s3:
#             /tuttle_twins/manifests/S01E01-manifest-2022-05-02T12:43:24.662714.jl
#         '''   
#     # create a temp file to hold the contents of the download
#     tmp_file = "/tmp/tmp-" + datetime.datetime.utcnow().isoformat()

#     manifest_rows = []
#     try:
#         # use bucket and key to download manifest json lines file to local tmp_file 
#         s3_download_text_file(manifest_bucket, manifest_key, tmp_file)

#         # The manifest file is a text file with a json string on each line.
#         # Decode each json_str into a json_dict and use ManifestRow to 
#         # verify its structure. Then append the manifest_row to list of all manifest_rows. 
#         with open(tmp_file, "r") as f:
#             for json_str in f:
#                 json_dict = json.loads(json_str)
#                 manifest_row = ManifestRow(json_dict)
#                 manifest_rows.append(manifest_row)
#     finally:
#         # delete the tmp_file whether exceptions were thrown or not
#         if os.path.exists(tmp_file):
#             os.remove(tmp_file)

#     return manifest_rows


# def download_latest_s3_manifest_file(season_code: str, episode_code: str) -> List[ManifestRow]:
#     '''
#     List the keys of all episode manifest files for the given season and 
#     episode in S3. Return an empty list if no keys were found.

#     Parse the 'version' string as the ending <utc_datetime_iso> 
#     portion of each key and add it as a new 'version' property of each key. 

#     Sort the list of keys by 'version' descending from highest to lowest.
#     The latest manifest_key is first in the sorted list.

#     Download and return the contents of latest manifest file as a list of 
#     ManifestRow dicts. 

#     Parameters
#     ----------
#         season_code (str): e.g. S01 for season 1
#         episode_code (str): e.g. E08 for episode 8
    
#     Returns
#     -------
#         An unordered list of ManifestRow describing the contents of the most recent episode manifest file 
#         for the given season and episode in S3. Return an empty list if no manifest file is found.
    
#     Notes
#     -------
#         Example s3 manifest filename:
#         S01E01-manifest-2022-05-03T22:53:30.325223.jl

#         Example aws cli command to find all episode manifest files for season 1 episode 1:
#         aws s3 ls s3://media.angel-nft.com/tuttle_twins/manifests/S01E01-manifest- | egrep ".*\.jl"

#         Example json string of an episode manifest file, with ManifestLine format:
#         {"src_url": "https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/stamps/TT_S01_E01_FRM-00-00-09-04.jpg", "dst_key": "tuttle_twins/s01e01/ML/validate/Legendary/TT_S01_E01_FRM-00-00-09-04.jpg"}
#     '''
#     bucket = S3_MEDIA_ANGEL_NFT_BUCKET
#     dir = "tuttle_twins/manifests"
#     prefix = se_code =  (season_code + episode_code).upper()
#     suffix = ".jl"

#     manifest_keys = s3_list_files(bucket=bucket, dir=dir, prefix=prefix, suffix=suffix, verbose=False)
#     if manifest_keys is None or len(manifest_keys) == 0:
#         logger.info(f"zero episode manifest files found for {se_code} in s3 - returning empty list")
#         return []

#     # Parse the 'version' string as the ending <utc_datetime_iso> 
#     # portion of each key and add it as a new 'version' property of each key. 
#     for manifest_key in manifest_keys:
#         version = parse_manifest_version(manifest_key)
#         manifest_key['version'] = version
    
#     # sort the manifest_keys array by the last_modified attribute, descending from latest to earliest
#     # the zero element is the latest key
#     latest_manifest_key = sorted(manifest_keys, key=lambda x: x['version'], reverse=True)[0]

#     # download the contents of the manifest file as a list of ManifestRow dicts
#     manifest_rows = s3_download_manifest_file(S3_MEDIA_ANGEL_NFT_BUCKET, latest_manifest_key)
#     return manifest_rows


# def create_manifest_files():
#     '''
#     Each "season_manifest_file", e.g. "S01-episodes.json" in s3 
#     is a JSON file that contains a list of "episode_objs". Each
#     "episode dict" is used to create a versioned "manifest file" 

#     These season_manifest_files are JSON files that are managed 
#     manually by members of the Angel Studios Data team. 
#     '''

#     all_episode_objs = download_all_s3_episode_objs

#     # create an episode manifest f_dfile for each episode dict
#     for episode in all_episode_objs:
#         create_versioned_manifest_file(episode)




# =============================================
# TESTS
# =============================================

def test_find_episode_keys():
    episode = Episode({"episode_id":"S01E01", "spreadsheet_title":"", "spreadsheet_url": "", "share_link":""})
    episode_keys_df = find_episode_keys(episode)
    assert set(episode_keys_df.columns) == set(['last_modified', 'size', 'key'])
    assert episode_keys_df is not None and len(episode_keys_df) > 0, "expected more than zero episode_keys_df"

def test_parse_manifest_version():
    episode_management_key = "s3://media.angel-nft.com/tuttle_twins/manifests/S01E01-manifest-2022-05-02T12:43:24.662714.jl"
    expected = "2022-05-02T12:43:24.662714"
    result = parse_manifest_version(episode_management_key)
    assert result == expected, f"unexpected result:{result}"

if __name__ == "__main__":
    test_parse_manifest_version()

