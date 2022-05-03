import typing
from typing import List, TypedDict
import sys
import re
import os
import boto3
from manifest import ManifestRow, LOCAL_MANIFESTS_DIR, S3_MEDIA_ANGEL_NFT_BUCKET, S3_PROCESS_BATCH_SIZE
import pandas as pd
import re
import json
from s3_utils import s3_copy_file


def process_manifest_jl_rows(rows: List[ManifestRow], row_idx: int=0) -> int:
    '''
    arguments:
      rows: a list of ManifestRow / TypedDict
      row_idx: the starting row count
    returns:
      the final row count
    '''
    dst_bucket = S3_MEDIA_ANGEL_NFT_BUCKET
    prog = re.compile(r"(https.+\.amazonaws\.com)/(.+\.com)/(.+\.jpg)")
    for row in rows:
        manifest_dict = json.loads(row)
        src_url = manifest_dict['src_url']
        dst_key = manifest_dict['dst_key']
        result = prog.match(src_url)
        src_bucket = result.group(2)
        src_key = result.group(3)
        assert src_bucket == S3_MEDIA_ANGEL_NFT_BUCKET, "wrong src_bucket!"

        response = s3_copy_file(src_bucket, src_key, dst_bucket, dst_key)
        httpStatusCode = response['ResponseMetadata']['HTTPStatusCode']
        assert httpStatusCode == 200, f"bad httpStatusCode: {httpStatusCode}"

        # # print rows with dst_keys with certain dst_classes only
        # if len(verbose_dst_classes) > 0:
        #     if any(dst_class in dst_key for dst_class in ["Rare","Legendary"]):
        #         print(f"row:{row_idx} s3_copy_file {src_bucket}/{src_key} => {dst_bucket}/{dst_key}")
        
        if row_idx > 0 and row_idx % 100 == 0:
            sys.stdout.write('.')
            if row_idx % 8000 == 0:
                sys.stdout.write('\n')
            sys.stdout.flush()

        row_idx += 1
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

    print(f"process_manifest_jl_file processed {row_idx} rows in {manifest_jl_file}")

    # return the total row count for this manifest_jl_file
    return row_idx


def process_manifest_jl_files():
    pattern = r'S\d\dE\d\d-manifest.*\.jl'
    manifest_jl_files = [f for f in os.listdir(LOCAL_MANIFESTS_DIR) if re.match(pattern, f)]
    for manifest_jl_file in manifest_jl_files:
        process_manifest_jl_file(manifest_jl_file)


if __name__ == "__main__":
    process_manifest_jl_files()


