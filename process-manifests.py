import typing
from typing import List, TypedDict
import sys
import re
import os
import boto3
from constants import ManifestRow, LOCAL_MANIFESTS_DIR, S3_MEDIA_ANGEL_NFT_BUCKET, S3_PROCESS_BATCH_SIZE
import pandas as pd
import re
import json
from timer_utils import func_timer
from s3_utils import s3_copy_file, s3_ls_recursive
import logging
logger = logging.getLogger(__name__)

def process_manifest_jl_rows(rows: List[ManifestRow], row_idx: int=0) -> int:
    '''
    copy the src_url to its dst_uri for each ManifestRow, 

    arguments:
      rows: a list of ManifestRow / TypedDict
      row_idx: the starting row count
    returns:
      the final row count
    '''
    num_fail = 0
    dst_bucket = S3_MEDIA_ANGEL_NFT_BUCKET

    # compiled regex matcher used to extract the src_bucket and src_key from the src_url in each row
    src_matcher = re.compile(r"https.+\.amazonaws\.com/(.+\.com)/(.+\.jpg)")

    for row in rows:
        manifest_dict = json.loads(row)
        src_url = manifest_dict['src_url']
        dst_key = manifest_dict['dst_key']
        result = src_matcher.match(src_url)
        src_bucket = result.group(1)
        src_key = result.group(2)
        assert src_bucket == S3_MEDIA_ANGEL_NFT_BUCKET, "wrong src_bucket!"
        assert src_key.endswith(".jpg"), f"illegal src_key:{src_key}"
        assert dst_key.endswith(".jpg"), f"illegal dst_key:{dst_key}"

        response = s3_copy_file(src_bucket, src_key, dst_bucket, dst_key)
        if len(response) > 0:
            httpStatusCode = response['ResponseMetadata']['HTTPStatusCode']
            assert httpStatusCode == 200, f"bad httpStatusCode: {httpStatusCode}"
        else:
            num_fail += 1  # track number of failed copies
        
        if row_idx > 0 and row_idx % 100 == 0:
            sys.stdout.write('.')
            if row_idx % 8000 == 0:
                sys.stdout.write('\n')
            sys.stdout.flush()

        row_idx += 1
    
    if num_fail > 0:
        logger.warn("process_manifest_jl_rows failed:{num_fail} passed:{row_idx-num_fail}")
    return row_idx

def process_manifest_jl_file(manifest_jl_file: str) -> str:
    '''
    Each season-episode manifest_jl file has thousands of json lines, for example:
    {"src_url": "https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/stamps/TT_S01_E01_FRM-00-00-08-15.jpg", 
    "dst_key": "tuttle_twins/s01e01/ML/train/Common/TT_S01_E01_FRM-00-00-08-15.jpg"}

    This function reads json lines into batches of batch_size or until all lines have been read.
    Each batch of json line/rows are processed by 'process_manifest_jl_rows'. 
    When all batches have been processed, the total number of procssed rows is returned.
    '''
    batch_size = S3_PROCESS_BATCH_SIZE
    row_idx = 0
    manifest_jl_path = os.path.join(LOCAL_MANIFESTS_DIR, manifest_jl_file)
    with open(manifest_jl_path,"r") as infile:
        rows = []
        for line in infile:
            rows.append(line)
            if len(rows) >= batch_size:
                row_idx = process_manifest_jl_rows(rows, row_idx)
                rows = []
        # handle the remnant
        if len(rows) > 0:
            row_idx = process_manifest_jl_rows(rows, row_idx)

    logger.info(f"process_manifest_jl_file processed {row_idx} rows in {manifest_jl_file}")

    # return the total ()row count for this manifest_jl_file
    return row_idx


def process_manifest_jl_files():
    pattern = r'S\d\dE\d\d-manifest.*\.jl'
    manifest_jl_files = [f for f in os.listdir(LOCAL_MANIFESTS_DIR) if re.match(pattern, f)]
    for manifest_jl_file in manifest_jl_files:
        process_manifest_jl_file(manifest_jl_file)

@s3_timer
def load_all_ML_s3_key_rows() -> List[s3_key]:
    '''
    do an exhaustive search over the ML (machine learning) folder in s3
    of all possible tuttle_twins seasons and episodes 
    e.g. aws s3 ls --recursive s3://media.angel-nft.com/tuttle_twins/<s01><e01>/ML/
    '''
    all_s3_key_rows = []
    num_seasons = 12
    num_episodes_per_season = 12
    for s in range(1,num_seasons+1):
        for e in range(1,num_episodes_per_season+1):
            se_key = f"{s:02}" + f"{e:02}"
            s3_key_rows  = s3_ls_recursive(f"s3://{S3_MEDIA_ANGEL_NFT_BUCKET}/tuttle_twins/{se_key}/ML/")
            if len(s3_key_rows) > 0:
                all_s3_key_rows.append(s3_key_rows)
    return all_s3_ML_key_rows

def upload_s3_manifest(List[ManifestRow]) -> None:
    pass

def  load_latest_s3_manifest_rows() -> List[ManifestRow]:
    '''
    search for the latest s3 manifest files for all possible tuttle_twins seasons and episodes 
    e.g. aws s3 ls --recursive s3://media.angel-nft.com/tuttle_twins/<s01><e01>/ML/
    '''
    all_s3_key_rows = []
    num_seasons = 1
    num_episodes_per_season = 12
    for s in range(1,num_seasons+1):
        for e in range(1,num_episodes_per_season+1):
            se_key = f"{s:02}" + f"{e:02}"

            s3_key_rows  = s3_ls_recursive(f"s3://{S3_MEDIA_ANGEL_NFT_BUCKET}/tuttle_twins/{se_key}/ML/")
            if len(s3_key_rows) > 0:
                all_s3_key_rows.append(s3_key_rows)
    return all_s3_ML_key_rows




def sync_all_season_episodes():
    '''
    find and download the latest version of each season-episode manifest_jl file from s3
    concatonate all manifests into one manifests_all
    compare all dst_keys in manifest against those in s3 listing
    '''
    ml_s3_key_rows = find_all_ML_s3_key_rows()
    all_manifests = gather_all_s3_manifests()


if __name__ == "__main__":
    process_manifest_jl_files()





