# see https://stackoverflow.com/a/35805441/18218031

import argparse
import os
import re
import sys
import subprocess
import datetime
from time import time, perf_counter
from typing import List
import boto3
from botocore.exceptions import ClientError
from s3_key import S3Key
from file_utils import generate_big_random_bin_file, compare_big_bin_files

AWS_REGION = "us-east-1"
s3_client = boto3.client('s3', region_name=AWS_REGION)
# s3_resource = boto3.resource('s3', region_name=AWS_REGION)
s3_resource = boto3.session.Session(region_name=AWS_REGION).resource("s3")

import logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger("s3_utils")

def s3_log_timer_info(func):
    '''
    decorator that shows execution time using this module's logger.debug
    '''
    def wrap_func(*args, **kwargs):
        t1 = perf_counter()
        result = func(*args, **kwargs)
        elapsed = perf_counter() - t1
        logger.debug(f"*** {func.__name__} executed in {elapsed:.6f}s ***")
        return result
    return wrap_func

def s3_copy_file(src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> dict:
    '''
    Copy a single s3 src object to s3 dst object
    Return the response dict
    '''
    if logger.getEffectiveLevel() <= logging.DEBUG:
        assert src_key is not None, f"ERROR: s3_copy_file() - src_key is undefined"
        assert dst_key is not None, f"ERROR: s3_copy_file() - dst_key is undefined"

    try:
        response = s3_client.copy_object(
            CopySource={'Bucket': src_bucket, 'Key': src_key}, 
            Bucket=dst_bucket, 
            Key=dst_key
        )            
        return response
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            logger.error(f"s3_copy_file() - NoSuchKey for src_key:{src_key} or dst_key:{dst_key} - returning empty")
            return dict()
        else:
            raise


@s3_log_timer_info
def s3_copy_files(src_bucket:str, src_keys: List[str], dst_bucket: str, dst_keys: List[str])-> None:
    '''
    Copy a list of s3 src objects to s3 dst objects
    '''
    logger.debug(f"s3_copy_files() {len(src_keys)} src files to {len(dst_keys)} destinations")
    try:
        zipped_keys = zip(src_keys, dst_keys)
        for src_key, dst_key in zipped_keys:
            s3_copy_file(src_bucket=src_bucket, src_key=src_key, dst_bucket=dst_bucket, dst_key=dst_key)
    except Exception as exp:
        logger.error(type(exp),str(exp))
        raise


def s3_delete_file(bucket: str, key: str) -> None:
    try:
        s3_resource.Object(bucket, key).delete()
    except Exception as exp:
        logger.error(type(exp),str(exp))
        raise


@s3_log_timer_info
def s3_delete_files(bucket: str, keys: List[str]) -> None:
    logger.debug(f"s3_delete_files() {len(keys)} files")
    for key in keys:
        s3_resource.Object(bucket, key).delete()


def s3_upload_file(up_path: str, bucket: str, channel: str):
    '''
    upload a text file to at up_path to s3
    '''
    up_file = os.path.basename(up_path)
    key = channel + "/" + up_file
    data = open(up_path, "rb")
    s3_resource.Bucket(bucket).put_object(Key=key, Body=data)


def s3_download_file(bucket: str, key: str, dn_path: str):
    '''
    download a text file from s3 into dn_path
    '''
    try:
        s3_resource.Bucket(bucket).download_file(key, dn_path)
    except Exception as exp:
        logger.error(type(exp), str(exp))
        raise


@s3_log_timer_info
def s3_list_files(bucket: str, dir: str, prefix: str=None, suffix: str=None, key_pattern: str=None, verbose: bool=False) -> List[dict]:
    '''
    returns a list of dict(data_modified, size and key) describing s3 object's that match the given search criteria
    '''
    prefix_str = "" if prefix is None else prefix + "*"
    suffix_str = "" if suffix is None else "*" + suffix
    key_pattern_str = "" if key_pattern is None else f" | egrep -e \"{key_pattern}\""

    s3_key_rows = []
    if verbose:
        logger.debug(f"something like: aws s3 ls s3://{bucket}/{dir}/{prefix_str}.*{suffix_str} | egrep {key_pattern_str}")

    if len(dir) > 0 and not dir.endswith("/"):
        dir += "/"

    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=dir )

    regex_key_pattern = re.compile(key_pattern) if key_pattern is not None else None

    num_found = 0
    for i, obj in enumerate(response["Contents"]):
        key = obj['Key']
        if prefix is None or prefix in key:
            if suffix is None or suffix in key:
                if regex_key_pattern is None or regex_key_pattern.search(key) is not None:

                    s3_key_dict = { "last_modified": obj['LastModified'], "size": obj['Size'], "key": key}
                    s3_key_rows.append(s3_key_dict)
                    if verbose:
                        print(key, '\t')
                    num_found += 1
    
    if verbose:
        logger.debug(f"s3_list_files() found:{len(s3_key_rows)}")

    return s3_key_rows

@s3_log_timer_info
def s3_ls_recursive(s3_uri: str) -> List[S3Key]:
    '''
    if given <s3_uri> "s3://media.angel-nft.com/tuttle_twins/ML/"
    makes system call "aws s3 ls --recursive <s3_uri> > <tmp_file>" 
    each line in <tmp_file>, e.g.
        "2022-05-03 19:15:44       2336 tuttle_twins/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"
    is parsed to create an S3Key object, which can be converted to dict, e.g.
        {"last_modified":"2022-05-03T19:15:44", "size":2336, "key":"tuttle_twins/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"}
    return the list of all S3Key as s3_key_listing
    '''
    try:
        s3_key_listing = []
        utc_datetime_iso = datetime.datetime.utcnow().isoformat()
        tmp_file = "/tmp/tmp-" + utc_datetime_iso
        cmd = "aws s3 ls --recursive " + s3_uri + " > " + tmp_file
        logger.debug(f"s3_ls_recursive() cmd:{cmd}")
        returned_value = subprocess.call(cmd, shell=True)  # returns the exit code in unix

        if returned_value != 0:
            logger.warn(f"s3_ls_recursive() subprocess exit code:{returned_value}")
        
        with open(tmp_file,"r") as f:
            for line in f:
                s3_key_row = S3Key(s3_ls_line=line)
                s3_key_listing.append(s3_key_row)
        
        logger.debug(f"s3_ls_recursive() {cmd} found:{len(s3_key_listing)}")

        return s3_key_listing
    finally:
        if os.path.exists(tmp_file):
            os.remove(tmp_file)

    
def parse_args(args):
    
    example = """
    python s3_utils.py media.angel-nft.com tuttle_twins/manifests --suffix .jl

    implements aws s3 ls s3://<bucket>/<dir>/[<prefix>]*[<suffix>]
    """
    parser = argparse.ArgumentParser(
        description='List files in S3.', 
        usage=f"--help/-h <bucket> <dir> [--prefix <prefix>] [--suffix <suffix>]\nexample: {example}")
    
    parser.add_argument('bucket', 
                        help='an s3 bucket')
    parser.add_argument('dir', 
                        help='an s3 dir/ (or dir/dir/)')
    parser.add_argument('--prefix', metavar="<prefix>",
                        help='an optional filename prefix')
    parser.add_argument('--suffix', metavar="<suffix>",
                        help='an optional filename suffix')
    parser.add_argument('--key_pattern', metavar="<key_pattern>",
                        help='an optional regex key pattern')
    parser.add_argument("--verbose", "-v", help="increase output verbosity",
                        action="store_true")
    try:
        return parser.parse_args(args)
    except Exception as exp:
        logger.error(f"{type(exp)} {str(exp)}")


def s3_list_file_cli(argv: List[str]) -> List[dict]:
    '''
    call s3_list_files() using command line arguments
    returns the number of files in S3 that match the given search criteria
    '''

    parser = parse_args(argv[1:])
    args = vars(parser)

    bucket = args['bucket']
    dir =  args['dir']
    prefix = args['prefix']
    suffix = args['suffix']
    key_pattern = args['key_pattern']
    verbose = args['verbose']
    
    logger.debug(f"bucket: {bucket}")
    logger.debug(f"dir: {dir}")
    logger.debug(f"prefix: {prefix}")
    logger.debug(f"suffix: {suffix}")
    logger.debug(f"key_pattern: {key_pattern}")
    logger.debug(f"verbose: {verbose}")

    s3_key_rows = s3_list_files(bucket=bucket, dir=dir, prefix=prefix, suffix=suffix, key_pattern=key_pattern, verbose=verbose)
    
    num_keys = len(s3_key_rows)
    print(f"found {num_keys} files")

    if num_keys < 10: 
        for s3_key_row in s3_key_rows:
            print(f" {s3_key_row['key']}\t{s3_key_row['size']} bytes\t{s3_key_row['last_modified'].isoformat()}")
    
    return s3_key_rows


if __name__ == "__main__":
    s3_list_file_cli(sys.argv)

