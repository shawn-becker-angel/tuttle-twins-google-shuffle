import pandas as pd
import numpy as np
import gspread
import os
from time import perf_counter
from shutil import copyfile
from typing import Dict, List
import random
from random import choices
import datetime
from episode import Episode
from file_utils import concatonate_file
from s3_key import get_S3Key_dict_list
from s3_utils import s3_log_timer_info, s3_ls_recursive, s3_delete_files, s3_copy_files
from season_service import download_all_seasons_episodes
from env import S3_MEDIA_ANGEL_NFT_BUCKET, GOOGLE_CREDENTIALS_FILE, DATA_FILES_DIR

import logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger("episode_service")

from logger_utils import set_all_info_loggers_to_debug_level

DATA_STAGES = ['train','test','pred']

_subsample_rate = 200

def set_subsample_rate(rate):
    rate = 0 if rate is None else int(rate)
    if rate >= 1: 
        global _subsample_rate
        _subsample_rate = rate;
    else:
        logger.debug(f"Invalid subsample_rate {rate} ignored.")
    logger.debug(f"subsample_rate: {_subsample_rate}")


def get_subsample_rate():
    return _subsample_rate

_verbosity_flag = False

def set_verbosity(flag :bool=False):
    _verbosity_flag = flag;
    if _verbosity_flag:
        set_all_info_loggers_to_debug_level()
    logger.debug(f"verbosity: {_verbosity_flag}")

def get_verbosity() -> bool:
    return _verbosity_flag

# ============================================

# episode_service overview
#
# episodes = season_service.load_all_season_episodes()

# --------------------------
# load all episodes from the episode JSON files stored within 
# the S3 manifests directory, set 'episode_id' as 
# 'season_code' + 'episode_code'
 
# G = find_sampled_google_episode_keys_df(episode)
# --------------------------
# use google sheet data for episode_id to define 
# G with columns [episode_id, img_src, img_frame, randomized new_ml_folder, manual new_ml_img_class]
# G -> [episode_id, img_src, img_frame, new_ml_key]

# C = s3_find_episode_jpg_keys_df(episode)
# -------------------------- 
# finds current jpg keys under tuttle_twins/ML with episode_id
# C with columns [last_modified, size, key, ml_folder, ml_image_class, img_frame, season_code, episode_code, episode_id]
# C -> columns [episode_id, img_frame, ml_key]

#-------------------------
# J1 = C join G on [episode_id, img_frame] to create
# J1 with columns [episode_id, img_frame, ml_key, nullable new_ml_key] 
# J1 discard rows where ml_key == new_ml_key
# J1 where new_ml_key is null delete ml_key file,
# J1 discard J1 rows with ml_key in del_keys
# J1 where ml_key != new_ml_key copy ml_key file to new_ml_key file, delete ml_key file,

#-----------------------------
# J2 = G join C on [episode_id, img_frame] to create
# J2 = columns=[episode_id, img_frame, new_ml_img_class, new_ml_folder, nullable ml_image_class, nullable ml_folder ] 
# J2 -> columns [episode_id, img_frame, new_ml_key, ml_key]

#-----------------------------
# J3 = J2 join S on [episode_id, img_frame] to create
# J3 with columns = [episode_id, img_frame, new_ml_key, ml_key, img_src]
# J3 where new_ml_key is not null and ml_key is null copy img_src file to new_ml_key file

#-----------------------------
# C4 =  fresh s3_find_episode_jpg_keys_df(episode)
# C4 with columns [last_modified, size, key, ml_folder, ml_image_class, img_frame, season_code, episode_code, episode_id]
# C4 -> columns [episode_id, img_frame, ml_key]

#-----------------------------
# G still has columns [episode_id, img_frame, new_ml_key, img_src]
# G2 -> G with columns [episode_id, img_frame, ml_key]

#-----------------------------
# assert C4 == G2
    
def add_randomized_new_ml_folder_column(df: pd.DataFrame) -> pd.DataFrame:
    '''
    per a percentage-wise distribution for episode_service.DATA_STAGES
    '''
    percentage_distribution = [
        ("train", 0.7),
        ("test", 0.2),
        ("pred", 0.1)
    ]
    data_stages = [item[0] for item in percentage_distribution]
    assert set(data_stages) == set(DATA_STAGES)

    choices = new_ml_folders = [x[0] for x in percentage_distribution]
    weights = new_percentages = [x[1] for x in percentage_distribution]
    k = len(df)

    df['new_ml_folder'] = random.choices(choices, weights=weights, k=k)
    return df

@s3_log_timer_info
def find_sampled_google_episode_keys_df(episode: Episode) -> pd.DataFrame:
    '''
    Read all rows of a "google episode sheet" described in the given episode into 
    dataframe G with columns=[episode_id, img_src, img_frame, manual new_ml_img_class]
    randomly sub-sample rows to keep 1 out of <subsmaple> rows, 
    then add use randomized new_ml_folder_column to define destination folders.
    '''
    episode_id = episode.get_episode_id()

    # use the google credentials file and the episode's google_spreadsheet_share_link to read
    # the raw contents of the first sheet into G
    gc = gspread.service_account(filename=GOOGLE_CREDENTIALS_FILE)
    gsheet = gc.open_by_url(episode.get_google_spreadsheet_share_link())
    data = gsheet.sheet1.get_all_records()
    df = pd.DataFrame(data)
    assert len(df) > 0, f"ERROR: google sheet df is empty"
    
    # subsample to keep only 1 out of <subsample_rate> rows
    num_subsampled_rows = round(len(df) / get_subsample_rate() )
    df = df.sample(num_subsampled_rows)

    # fetch the public 's3_thumbnails_base_url' from the name of column zero
    # e.g. https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/
    s3_thumbnails_base_url = df.columns[0]
    
    # parse out 's3_img_src_base' from 's3_thumbnails_base_url'
    # e.g. "tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/"
    tt_idx = s3_thumbnails_base_url.find("tuttle_twins")
    s3_img_src_base = s3_thumbnails_base_url[tt_idx:]

    # verify that s3_img_src_base_url contains episode_id.lower() e.g. s01e01
    episode_id_lower = episode_id.lower()
    if s3_img_src_base.find(episode_id_lower) == -1:
        raise Exception(f"s3_img_src_base fails to include {episode_id_lower}")

    # verify that all rows of the "FRAME NUMBER" column contain the 'episode_frame_code', e.g. 
    #   "TT_S01_E01_FRM"  
    # example FRAME_NUMBER column: 
    #   "TT_S01_E01_FRM-00-00-08-11"
    episode_frame_code = ("TT_" + episode.get_split_episode_id() + "_FRM").upper()
    matches = df[df['FRAME NUMBER'].str.contains(episode_frame_code, case=False)]
    failure_count = len(df) - len(matches)
    if failure_count > 0:
        raise Exception(f"{failure_count} rows have FRAME NUMBER values that don't contain 'episode_frame_code': {episode_frame_code}" )

    # save the episode_id in all rows
    df['episode_id'] = episode_id

    # store the "FRAME_NUMBER" column as "img_frame"
    df['img_frame'] = df["FRAME NUMBER"]

    # compute the "img_url" column of each row using the s3_thumbnails_base_url and the 'img_frame' of that row
    df['img_src'] = s3_img_src_base + df['img_frame'] + ".jpg"

    # compute the "new_ml_img_class" column as the first available "CLASSIFICATION" for that row or None
    df['new_ml_img_class'] = \
        np.where(df["JONNY's RECLASSIFICATION"].str.len() > 0, df["JONNY's RECLASSIFICATION"],
        np.where(df["SUPERVISED CLASSIFICATION"].str.len() > 0, df["SUPERVISED CLASSIFICATION"],
        np.where(df["UNSUPERVISED CLASSIFICATION"].str.len() > 0, df["UNSUPERVISED CLASSIFICATION"], None)))
    
    # add the randomized column 'new_ml_folder'
    df = add_randomized_new_ml_folder_column(df)
    
    # new_ml_key = new_ml_folder / new_img_class
    df['new_ml_key'] = np.where(~df['new_ml_folder'].isnull() & ~df['new_ml_img_class'].isnull(), df['new_ml_folder'] + '/' + df['new_ml_img_class'], None)

    # keep only these columns
    df = df[['episode_id', 'img_src','img_frame', 'new_ml_key']]

    logger.debug(f"find_sampled_google_episode_keys_df() episode_id:{episode_id} df.shape:{df.shape}")
    return df


def create_google_episode_stage_data_files(episode: Episode) -> Dict[str,str]:
    '''
    Use the find_sampled_google_episode_keys_df to create the episode_stage_data_file
    for each stage is (DATA_STAGES: [train, test, pred] 
    with columns 'file_name' and 'label'
    ''' 
    episode_stage_data_files = {}

    G = find_sampled_google_episode_keys_df(episode)
    G = G[['img_frame', 'new_ml_key']]

    # create datafile S for each stage with columns 
    # 'file_name' and 'label', for each image row
    # and save S to episode_stage_data_file.

    # create 'file_name' column for each image row by adding ".jpg"
    G['file_name'] = G['img_frame'] + ".jpg"

    # new_ml_key = new_ml_folder / new_img_class
    # split 'new_ml_key' on '/' to create new 'new_ml_folder' and 'new_img_class' columns
    # but use 'stage' instead of 'new_ml_folder'
    # and use 'label' instead of 'new_img_class'
    new_ml_key_cols = ['stage', 'label']
    new_ml_key_df = G['new_ml_key'].str.split('/', expand=True).rename(columns = lambda x: new_ml_key_cols[x])

    # concat the new new_ml_key_df columns to G
    G = pd.concat([G, new_ml_key_df], axis=1)
    
    # verify that G has the correct set of stages
    result = set(G['stage'].unique())
    expected = set(DATA_STAGES)
    assert result == expected, F"ERROR: expected stages: {expected} not {result}"

    dt = datetime.datetime.utcnow().isoformat()
    ss = get_subsample_rate()

    # filter on each stage to create an episode_stage_data_file
    for stage in DATA_STAGES:
        # S holds all rows of G with a given stage
        S = G[G['stage'] == stage]

        # keep only file_name and label
        S = S[['file_name','label']]

        # save S to episode_stage_data_file
        episode_code = episode.get_episode_code()
        episode_stage_data_file = f"{DATA_FILES_DIR}/{episode_code}_{stage}_{dt}_{ss}_data.csv"

        if not os.path.isdir(DATA_FILES_DIR):
            os.mkdir(DATA_FILES_DIR)

        S.to_csv(episode_stage_data_file, header=False, index=False, line_terminator='\n')

        # update the dict of all episode_stage_data_files 
        episode_stage_data_files[stage] = episode_stage_data_file

    return episode_stage_data_files
        

def split_key_in_df(df: pd.DataFrame) -> pd.DataFrame:
    '''
    split df.key to add columns ['ml_folder', 'ml_image_class', 'img_frame', 'season_code', 'episode_code', 'episode_id']
    which coalesce to add columns ['ml_key', 'img_frame', 'episode_id']
    '''
    # e.g. df.long = "tuttle_twins/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"

    # added last 'n' cols in case df['key' has trailing text?
    key_cols = ['tt','ml','ml_folder','ml_image_class','img_frame','ext','n1','n2']
    key_df = df['key'].str.split('/|\.', expand=True).rename(columns = lambda x: key_cols[x])
    
    # e.g. key_df.ml_folder = "validate"
    # e.g. key_df.ml_image_class = "Uncommon"
    # e.g. key_df.img_frame = "TT_S01_E01_FRM-00-19-16-19"

    # e.g. key_df.ml_key = "validate/Unommon"
    key_df['ml_key'] = np.where(~key_df['ml_folder'].isnull() & ~key_df['ml_image_class'].isnull(), key_df['ml_folder'] + '/' + key_df['ml_image_class'], None)
    key_df = key_df[['ml_key','img_frame']]

    df = pd.concat([df,key_df], axis=1)
    
    img_frame_cols = ['tt', 'season_code', 'episode_code', 'remainder']
    img_frame_df = df.img_frame.str.split('_', expand=True).rename(columns = lambda x: img_frame_cols[x])
    img_frame_df.drop(columns=['tt','remainder'], axis=1, inplace=True)
    
    # e.g. img_frame.season_code = "S01"
    # e.g. img_frame.episode_code = "E01"
    
    expected = set(['season_code','episode_code'])
    result = set(img_frame_df.columns)
    assert result == expected, f"ERROR: expected img_frame_df.columns: {expected} not {result}"
    
    # e.g. img_frame.episode_id = "S01E01"
    
    img_frame_df['episode_id'] = np.where(~img_frame_df['season_code'].isnull() & ~img_frame_df['episode_code'].isnull(), img_frame_df['season_code'] + img_frame_df['episode_code'], None)
    img_frame_df = img_frame_df[['episode_id']]

    df = pd.concat([df,img_frame_df], axis=1)
    
    return df

@s3_log_timer_info
def s3_find_episode_jpg_keys_df(episode: Episode) -> pd.DataFrame:
    '''
    find current jpg keys under tuttle_twins/ML with episode_id
    C = columns=[last_modified, size, key, ml_folder, ml_image_class, img_frame, season_code, episode_code, episode_id]
    C -> columns=[episode_id, img_frame, ml_key]
    '''
    bucket = S3_MEDIA_ANGEL_NFT_BUCKET
    dir = "tuttle_twins/ML"
    suffix = ".jpg"
    
    # example episode_id: S01E01, split_episode_id: S01_E01
    episode_id = episode.get_episode_id()

    # example key: tuttle_twins/ML/validate/Rare/TT_S01_E01_FRM-00-00-09-01.jpg
    episode_key_pattern = f"TT_{episode.get_split_episode_id()}_FRM-.+\.jpg"
    s3_uri = f"s3://media.angel-nft.com/tuttle_twins/ML/ | egrep -e \"{episode_key_pattern}\""
    s3keys_list = s3_ls_recursive(s3_uri)
    if len(s3keys_list) == 0:
        logger.debug(f"episode_id:{episode_id} zero episode_jpg_keys found.")
        return pd.DataFrame()

    # convert list of S3Key to list of S3Key.as_dict
    s3key_dict_list = get_S3Key_dict_list.__func__(s3keys_list)

    # create dataframe with columns ['last_modified', 'size', 'key']
    df = pd.DataFrame(s3key_dict_list, columns=['last_modified', 'size', 'key'])
    
    # split df.key to add columns ['ml_key', 'img_frame', 'season_code', 'episode_code', 'episode_id']
    df = split_key_in_df(df)
    
    df['episode_id'] = episode_id
    num_ne = len(df[df['episode_id'].ne(episode_id)])
    assert num_ne == 0, f"ERROR: {num_ne} rows don't have episode_id"

    df = df[['episode_id', 'img_frame', 'ml_key']]
    
    logger.debug(f"s3_find_episode_jpg_keys_df() episode_id:{episode_id} df.shape:{df.shape}")
    return df

def log_progress(prefix, episode_id, action, num_files, num_sec, files_per_sec):
    logger.debug(f"{prefix} episode_id:{episode_id} {action} - num_files:{num_files} num_sec:{num_sec} rate:{files_per_sec:.3f} files/sec")

def process_episode(episode: Episode) -> None:
    '''Do everything required to process the given episode'''

    episode_id = episode.get_episode_id()
    total_files_needed = 0
    num_files_deleted = 0
    num_files_moved = 0
    num_files_copied = 0

    #-----------------------------
    # G is files needed at new_ml_key
    G = find_sampled_google_episode_keys_df(episode)
    if len(G) == 0:
        logger.debug("find_sampled_google_episode_keys_df() episode_id:{episode_id} zero rows found. Skipping this episode")
        return
    
    total_files_needed = len(G)
    logger.debug(f">>> episode_id:{episode_id} total files needed in s3: {total_files_needed}")

    expected = set(['episode_id', 'img_src', 'img_frame', 'new_ml_key'])
    result = set(G.columns)
    assert result == expected, f"ERROR: expected G.columns: {expected} not {result}"

    # --------------------------
    # C is files currently at ml_key
    C = s3_find_episode_jpg_keys_df(episode)
    if len(C) > 0:
        expected = set(C[['episode_id', 'img_frame', 'ml_key']])
        result = set(C.columns)
        assert result == expected, f"ERROR: expected C.columns: {expected} not {result}"
    
    # NOTE:
    #   if C has been pre-loaded and G has been significantly subsampled
    #   C should be much larger than G
    #   conversely, if C has never been loaded, then C should be zero
    #   and G should be larger than C
    logger.debug(f"len(C): {len(C)}")
    logger.debug(f"len(G): {len(G)}")

    #-------------------------
    # J1 maps ml_key from C that needs to be at new_ml_key from G
    # J1 = C join G on [episode_id, img_frame] to create
    # J1 with columns [episode_id, img_frame, ml_key, nullable new_ml_key] 
    if len(C) > 0:       
        J1 = C.merge(G,  
            how='left', 
            on=['episode_id', 'img_frame'], 
            suffixes=('',''),
            sort=False)
        
    # if C is empty then set J1 to G + null ml_key
    # because no ml_key were found
    else:
        J1 = G.copy(deep=True)
        J1['ml_key'] = None

    expected = set(['episode_id', 'img_frame', 'ml_key', 'img_src', 'new_ml_key'])
    result = set(J1.columns)
    assert result == expected, f"ERROR: expected J1.columns: {expected} not {result}"
            
    # keep only rows where ml_key != new_ml_key - because these require action
    J1 = J1[J1['ml_key'].ne(J1['new_ml_key'])]

    # J1_del are files in ml_key that have no new_ml_key, so ml_key needs to be deleted
    # J1_del is J1 where ml_key is not null and new_ml_key is null so delete ml_key file 
    # NOTE: 
    #   J1_del is very large if ml_key has been preloaded and
    #   G has been significantly subsampled
    #   conversely, J1_del is zero if C is zero
    J1_del = J1[~J1['ml_key'].isnull() & J1['new_ml_key'].isnull()] 
    if len(J1_del) > 0:
        # tuttle_twins/ML/validate/Rare/TT_S01_E01_FRM-00-00-09-01.jpg 
        J1_del['del_key'] = "tuttle_twins/ML/" + J1_del['ml_key'] + '/' + J1_del['img_frame'] + ".jpg"
        del_keys = list(J1_del['del_key'].to_numpy())

        del_start = perf_counter()
        s3_delete_files(bucket=S3_MEDIA_ANGEL_NFT_BUCKET, keys=del_keys)

        action = "files deleted from ML"
        num_files_deleted = len(del_keys)
        num_sec = perf_counter() - del_start
        files_per_sec = num_files_deleted / num_sec
        log_progress(">>>", episode_id, action, num_files_deleted, num_sec, files_per_sec)

        # keep only J1 rows with ml_key not in del_keys
        J1 = J1[~J1['ml_key'].isin(del_keys)]

    # J1_mv are files in ml_key that need to be moved to new_ml_key
    # J1_mv is J1 where ml_key != new_ml_key copy ml_key file to new_ml_key file, delete ml_key file
    # NOTE:
    #   J1_mv is very small if ml_key has been preloaded and
    #   G has been significantly subsampled
    #   conversely, J1_mv is zero if C is zero
    J1_mv = J1[~J1['ml_key'].isnull() & ~J1['new_ml_key'].isnull()]
    if len(J1_mv) > 0:
        
        '''
        episode_service.py:340: SettingWithCopyWarning: 
        A value is trying to be set on a copy of a slice from a DataFrame.
        Try using .loc[row_indexer,col_indexer] = value instead

        See the caveats in the documentation: https://pandas.pydata.org/pandas-docs/stable/user_guide/indexing.html#returning-a-view-versus-a-copy
        J1_mv['src_key'] = "tuttle_twins/ML/" + J1_mv['ml_key'] + '/' + J1_mv['img_frame'] + ".jpg"
        '''
        
        # J1_mv['src_key'] = 
        J1_mv.loc[:,'src_key'] = \
            "tuttle_twins/ML/" + J1_mv['ml_key'] + '/' + J1_mv['img_frame'] + ".jpg"

        # J1_mv['dst_key'] = 
        J1_mv.loc[:,'dst_key'] = \
            "tuttle_twins/ML/" + J1_mv['new_ml_key'] + '/' + J1_mv['img_frame'] + ".jpg"
        
        J1_mv = J1_mv[['src_key','dst_key']]
        src_keys = list(J1_mv['src_key'].to_numpy())
        dst_keys = list(J1_mv['dst_key'].to_numpy())

        mv_start = perf_counter()
        # mv part 1 - copy src_key to dst_key
        s3_copy_files(src_bucket=S3_MEDIA_ANGEL_NFT_BUCKET, src_keys=src_keys,
                        dst_bucket=S3_MEDIA_ANGEL_NFT_BUCKET, dst_keys=dst_keys)
        
        # mv part 2 - delete src_key
        del_keys = src_keys
        s3_delete_files(bucket=S3_MEDIA_ANGEL_NFT_BUCKET, keys=del_keys)

        action = "files moved from ML to ML"
        num_files_moved = len(del_keys)
        num_sec = perf_counter() - mv_start
        files_per_sec = num_files_moved / num_sec
        log_progress(">>>", episode_id, action, num_files_moved, num_sec, files_per_sec)
    
    #-----------------------------
    # J2 = G2 join C2 on [episode_id, img_frame] to create
    # J2 = columns=[episode_id, img_frame, new_ml_key, nullable ml_key ] 
    
    # G2 is files needed at new_ml_key
    # G2 is G without img_src
    G2 = G[['episode_id', 'img_frame', 'new_ml_key']]

    # C2 is a fresh search of files still at ml_key
    C2 = s3_find_episode_jpg_keys_df(episode)

    # FIX J2
    # have len(C2) is 28077 and only need len(G2) is 281  
    # so should delete 28077 - 281

    if len(C2) > 0:
        J2 = G2.merge(C2,  
            how='left', 
            on=['episode_id', 'img_frame'], 
            suffixes=('',''),
            sort=False)
    # if C2 is empty then set J2 to G2 + null ml_key
    else:
        J2 = G2.copy(deep=True)
        J2['ml_key'] = None

    expected = set(['episode_id', 'img_frame', 'new_ml_key','ml_key'])
    result = set(J2.columns)
    assert result == expected, f"ERROR: expected J2.columns: {expected} not {result}"
    
    # J2 keep only rows where new_ml_key != ml_key, because that's where changes are needed
    J2 = J2[J2['new_ml_key'].ne(J2['ml_key'])]

    #-----------------------------
    # G3 keeps img_src
    G3 = G[['episode_id', 'img_frame', 'img_src', 'new_ml_key']]

    # J3 = J2 join G3 on [episode_id, img_frame] to create
    # J3 with columns = [episode_id, img_frame, new_ml_key, ml_key, img_src]
    if len(J2) > 0:
        J3 = J2.merge(G3,  
            how='left', 
            on=['episode_id', 'img_frame', 'new_ml_key'], 
            suffixes=('',''),
            sort=False)
    # if J2 is empty then J3 is G + null ml_key
    else:
        J3 = G.copy(deep=True)
        J3['ml_key'] = None
    
    expected = set(['episode_id', 'img_frame', 'new_ml_key', 'ml_key', 'img_src'])
    result = set(J3.columns)
    assert result == expected, f"ERROR: expected J3.columns: {expected} not {result}"

    # J3 where new_ml_key is not null and ml_key is null copy img_src file to new_ml_key file
    J3_cp = J3[~J3['new_ml_key'].isnull() & J3['ml_key'].isnull()]
    
    if len(J3_cp) > 0:
        # e.g. J3_cp['dst_file'] = "tuttle_twins/ML/" + "training/Common" + "/" + "TT_S01_E01_FRM-00-18-21-08" + ".jpg"
        J3_cp['dst_file'] = "tuttle_twins/ML/" + J3_cp['new_ml_key'] + '/' + J3_cp['img_frame'] + ".jpg"
        src_keys = J3_cp['img_src']
        dst_keys = J3_cp['dst_file']
        
        cp_start = perf_counter()
        s3_copy_files(src_bucket=S3_MEDIA_ANGEL_NFT_BUCKET, src_keys=src_keys, 
                        dst_bucket=S3_MEDIA_ANGEL_NFT_BUCKET, dst_keys=dst_keys)

        action = "files copied from src to ML"
        num_files_copied = len(src_keys)
        num_sec = perf_counter() - cp_start
        files_per_sec = num_files_copied / num_sec
        log_progress(">>>", episode_id, action, num_files_copied, num_sec, files_per_sec)

    #-----------------------------
    # C4 = fresh s3_find_episode_jpg_keys_df(episode)
    # C4 with columns [last_modified, size, ml_key, ml_folder, ml_image_class, img_frame, season_code, episode_code, episode_id]
    C4 = s3_find_episode_jpg_keys_df(episode)
    if len(C4) == 0:
        raise Exception("zero jpg files found for episode_id:{episode_id} should not be possible")

    expected = set(['episode_id', 'img_frame', 'ml_key'])
    result = set(C4.columns)
    assert result == expected, f"ERROR: expected C4.columns: {expected} not {result}"
    
    #-----------------------------
    # G still has columns [episode_id, img_frame, img_src, new_ml_key]
    # G2 is the cropped and renamed version of G, so it can be compared with C4
    # G2 -> G with columns [episode_id, img_frame, ml_key]
    G2 = G[['episode_id', 'img_frame', 'new_ml_key']]
    G2 = G2.rename(columns={'new_ml_key' :'ml_key'})
    expected = set(['episode_id', 'img_frame', 'ml_key'])
    result = set(G2.columns)
    assert result == expected, f"ERROR: expected G2.columns: {expected} not {result}"

    logger.debug(f"episode_id: {episode_id} num files needed in ML: {total_files_needed}")
    logger.debug(f"episode_id: {episode_id} num files deleted from ML: {num_files_deleted}")
    logger.debug(f"episode_id: {episode_id} um files moved within ML: {num_files_moved}")
    logger.debug(f"episode_id: {episode_id} num files copied from src: {num_files_copied}")
    num_files_unchanged = total_files_needed - num_files_moved - max(num_files_deleted, num_files_copied)
    logger.debug(f"episode_id: {episode_id} num files unchanged: {num_files_unchanged}")

    #-----------------------------
    # assert C4 == G2
    expected = G2.shape
    result = C4.shape
    if result != expected:
        logger.debug(f"episode_id: {episode_id} final shape result: {result} != shape expected: {expected}")
    else:
        logger.debug(f"episode_id: {episode_id} final shape result: {result} == shape expected: {expected}")

def process_all_episodes() -> None:
    all_episodes = download_all_seasons_episodes()
    for episode in all_episodes:
        process_episode(episode)

def get_all_season_codes() -> List[str]:
    all_season_codes = set()
    all_episodes = download_all_seasons_episodes()
    for episode in all_episodes:
        all_season_codes.add(episode.get_season_code())
    return sorted(list(all_season_codes))

def create_all_stage_data_files(subsample_rate :int=100, cleanup: bool=True, verbosity :bool=False) -> Dict[str,str]:
    '''
    This is the main entry point for create_date_files.py

    concat the contents all episode stage files
    into a single set of stage_data_files

    reports information about the settings used to create the set of data files
    
    if cleanup then remove all intermediate datafiles
    
    returns the final list of unstamped stage datafiles
    '''

    set_subsample_rate(subsample_rate)
    set_verbosity(verbosity)

    all_unstamped_stage_data_files = {}
    all_stamped_stage_data_files = {}
    all_episode_stage_data_files = []
    all_episodes = download_all_seasons_episodes()

    dt = datetime.datetime.utcnow().isoformat()
    ss = get_subsample_rate()

    # get all episodes of all season manifest files found in s3
    for episode in all_episodes:
        # concatonate the contents of all episode_stage_data_files by stage into all_stamped_stage_data_files
        episode_stage_data_files = create_google_episode_stage_data_files(episode=episode)
        all_episode_stage_data_files.extend(list(episode_stage_data_files.values()))
        logger.warning(f"*** episode_code:{episode.get_episode_code()} all_episode_stage_data_files.length: {len(all_episode_stage_data_files)}")
        for stage in DATA_STAGES:
            stamped_stage_data_file = f"{DATA_FILES_DIR}/{stage}_{dt}_{ss}_data.csv"
            concatonate_file( src_file=episode_stage_data_files[stage], dst_file=stamped_stage_data_file)
            all_stamped_stage_data_files[stage] = stamped_stage_data_file
            
    for stage in DATA_STAGES:
        # copy all stamped_stage_data_file to unstamped_stage_data_file
        stamped_stage_data_file = all_stamped_stage_data_files[stage]
        unstamped_stage_data_file = f"{DATA_FILES_DIR}/{stage}_data.csv"
        copyfile(stamped_stage_data_file, unstamped_stage_data_file)
        all_unstamped_stage_data_files[stage] = unstamped_stage_data_file
    
    # remove all intermediate data files            
    if cleanup:
        logger.warning(f"*** all_episode_stage_data_files.length: {len(all_episode_stage_data_files)}")
        for episode_stage_data_file in all_episode_stage_data_files:
            os.remove(episode_stage_data_file)
        for stage in DATA_STAGES:
            os.remove(all_stamped_stage_data_files[stage])
    
    # return only the 'unstamped' stage data files
    return all_unstamped_stage_data_files
    

# =============================================
# TESTS
# =============================================

def get_test_episode():
    episode_dict = {
        "season_code": "S01",
        "episode_code": "E02",
        "google_spreadsheet_title": "Tuttle Twins S01E02 Unsupervised Clustering",
        "google_spreadsheet_url": "https://docs.google.com/spreadsheets/d/1v40TwUEphfX174xbAE-L3ORKqRz7S_jKeSeilibnkqQ/edit#gid=1690818184",
        "google_spreadsheet_share_link": "https://docs.google.com/spreadsheets/d/1v40TwUEphfX174xbAE-L3ORKqRz7S_jKeSeilibnkqQ/edit?usp=sharing"
    }

    episode = Episode(episode_dict)
    return episode

def test_find_sampled_google_episode_keys_df():
    episode = get_test_episode()
    episode_id = episode.get_episode_id()
    G = find_sampled_google_episode_keys_df(episode)
    if len(G) > 0:
        expected = set(['episode_id', 'img_src', 'img_frame', 'new_ml_key'])
        result = set(G.columns)
        assert result == expected, f"ERROR: expected G.columns: {expected} not {result}"
    else:
        logger.debug(f"Zero google episode keys found for episode_id:{episode_id}")

def test_s3_find_episode_jpg_keys_df():
    episode = get_test_episode()
    episode_id = episode.get_episode_id()
    C = s3_find_episode_jpg_keys_df(episode)
    if len(C) > 0:
        expected = set(C[['episode_id', 'img_frame', 'ml_key']])
        result = set(C.columns)
        assert result == expected, f"ERROR: expected C.columns: {expected} not {result}"
    else:
        logger.debug(f"Zero episode jpg keys found for episode_id:{episode_id}")

def test_create_google_episode_stage_data_files():
    episode = get_test_episode()
    episode_stage_data_files = create_google_episode_stage_data_files(episode=episode)
    for stage in DATA_STAGES:
        episode_stage_data_files[stage] is not None

def test_create_all_stage_data_files():
    result = create_all_stage_data_files()
    assert len(result) == len(DATA_STAGES)


if __name__ == "__main__":
    set_all_info_loggers_to_debug_level()
    test_find_sampled_google_episode_keys_df()
    test_create_google_episode_stage_data_files()
    test_s3_find_episode_jpg_keys_df()
    test_create_all_stage_data_files()


    logger.debug("done")

