# see https://stackoverflow.com/a/35805441/18218031

import argparse
import os
import re
import sys
import filecmp
import subprocess
import datetime
from time import perf_counter
from typing import List, Tuple
import boto3
from botocore.exceptions import ClientError
from time import perf_counter
from s3_key import s3_key

s3_client = boto3.client('s3')
s3_resource = boto3.resource('s3')

import logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger("s3_utils")

def s3_log_timer_info(func):
    '''
    decorator that shows execution time using this module's logger.info
    '''
    def wrap_func(*args, **kwargs):
        t1 = perf_counter()
        result = func(*args, **kwargs)
        elapsed = perf_counter() - t1
        logger.info(f"*** {func.__name__} executed in {elapsed:.6f}s ***")
        return result
    return wrap_func


def s3_file_exists(bucket: str, key: str):
    '''Return True if s3 file exists'''
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError:
        return False


def s3_copy_file(src_bucket: str, src_key: str, dst_bucket: str, dst_key: str) -> dict:
    '''Return the response dict'''
    try:
        response = s3_client.copy_object(
            CopySource={'Bucket': src_bucket, 'Key': src_key}, 
            Bucket=dst_bucket, 
            Key=dst_key
        )
        return response
    except ClientError as ex:
        if ex.response['Error']['Code'] == 'NoSuchKey':
            logger.info(f"s3_copy_file() - No object found for src_key:{src_key} or dst_key:{dst_key} - returning empty")
            return dict()
        else:
            raise


@s3_log_timer_info
def s3_copy_files(src_bucket:str, src_keys: List[str], dst_bucket: str, dst_keys: List[str])-> None:
    logger.info(f"s3_copy_files() {len(src_keys)} src files to {len(dst_keys)} destinations")
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
    logger.info(f"s3_delete_files() {len(keys)} files")
    for key in keys:
        s3_resource.Object(bucket, key).delete()


def s3_upload_text_file(up_path: str, bucket: str, channel: str):
    '''
    upload a text file to at up_path to s3
    '''
    up_file = os.path.basename(up_path)
    key = channel + "/" + up_file
    data = open(up_path, "rb")
    s3_resource.Bucket(bucket).put_object(Key=key, Body=data)


def s3_download_text_file(bucket: str, key: str, dn_path: str):
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
    returns a list of s3_key describing s3 object that match the given search criteria
    '''
    prefix_str = "" if prefix is None else prefix + "*"
    suffix_str = "" if suffix is None else "*" + suffix
    key_pattern_str = "" if key_pattern is None else f" | egrep \"{key_pattern}\""

    s3_key_rows = []
    if verbose:
        logger.info(f"something like: aws s3 ls s3://{bucket}/{dir}/{prefix_str}.*{suffix_str} | egrep {key_pattern_str}")

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
        logger.info(f"s3_list_files() found:{len(s3_key_rows)}")

    return s3_key_rows


@s3_log_timer_info
def s3_list_file_cli():
    '''
    call s3_lis= t_files() using command line arguments
    returns the number of files in S3 that match the given search criteria
    '''
    example = """
    python s3_utils media.angel-nft.com tuttle_twins/manifests --suffix .jl

    implements aws s3 ls s3://<bucket>/<dir>/[<prefix>]*[<suffix>]
    """
    parser = argparse.ArgumentParser(description='List files in S3.', usage=f"--help/-h <bucket> <dir> [--prefix <prefix>] [--suffix <suffix>]\nexample: {example}")
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

    args = vars(parser.parse_args())

    bucket = args['bucket']
    dir =  args['dir']
    prefix = args['prefix']
    suffix = args['suffix']
    key_pattern = args['key_pattern']
    verbose = args['verbose']

    s3_key_rows = s3_list_files(bucket=bucket, dir=dir, prefix=prefix, suffix=suffix, key_pattern=key_pattern, verbose=verbose)
    return len(s3_key_rows)


@s3_log_timer_info
def s3_ls_recursive(s3_uri: str) -> List[s3_key]:
    '''
    if given <s3_uri> "s3://media.angel-nft.com/tuttle_twins/ML/"
    makes system call "aws s3 ls --recursive <s3_uri> > <tmp_file>" 
    each line in <tmp_file>, e.g.
        "2022-05-03 19:15:44       2336 tuttle_twins/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"
    is parsed to create an s3_key object, which can be converted to dict, e.g.
        {"last_modified":"2022-05-03T19:15:44", "size":2336, "key":"tuttle_twins/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"}
    return the list of all s3_key as s3_key_listing
    '''
    s3_key_listing = []
    utc_datetime_iso = datetime.datetime.utcnow().isoformat()
    tmp_file = "/tmp/tmp-" + utc_datetime_iso
    cmd = "aws s3 ls --recursive " + s3_uri + " > " + tmp_file
    logger.info(f"s3_ls_recursive() cmd:{cmd}")
    returned_value = subprocess.call(cmd, shell=True)  # returns the exit code in unix

    if returned_value != 0:
        logger.error(f"subprocess exit code:{returned_value} - returning empty list")
        return []
    
    with open(tmp_file,"r") as f:
        for line in f:
            s3_key_row = s3_key(s3_ls_line=line)
            s3_key_listing.append(s3_key_row.as_dict())
    
    os.remove(tmp_file)
    logger.info(f"s3_ls_recursive() found:{len(s3_key_listing)}")

    return s3_key_listing
    

###################################################
# TESTS
###################################################
    
def test_s3_copy_file():
    '''
    "src_url": "s3://media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/TT_S01_E01_FRM-00-00-00-03.jpg", 
    "src_key": "tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/TT_S01_E01_FRM-00-00-00-03.jpg", 
    "dst_key": "tuttle_twins/ML/deleteme/test.jpg"
    '''
    src_bucket = "media.angel-nft.com"
    src_key = "tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/TT_S01_E01_FRM-00-00-00-03.jpg"
    dst_bucket = "media.angel-nft.com"
    dst_key = "tuttle_twins/ML/deleteme/test.jpg"

    assert s3_file_exists(src_bucket, src_key), f"ERROR: src file does not exist"

    response = s3_copy_file(src_bucket, src_key, dst_bucket, dst_key)
    assert 'ResponseMetadata' in response, f"ERROR: no ResponseMetaData key"
    assert 'HTTPStatusCode' in response['ResponseMetadata'], f"ERROR: no HTTPStatusCode key"
    httpStatusCode = response['ResponseMetadata']['HTTPStatusCode']
    assert httpStatusCode == 200, f"ERROR: bad httpStatusCode: {httpStatusCode}"

    assert s3_file_exists(dst_bucket, dst_key), f"ERROR: dst file does not exist"
    s3_delete_file(dst_bucket, dst_key)
    assert not s3_file_exists(dst_bucket, dst_key), f"ERROR: dst file not deleted"


def test_s3_upload_download():
    tmp_dir = "/tmp"
    test_up_path = os.path.join(tmp_dir, "test_up.txt")
    test_dn_path = os.path.join(tmp_dir, "test_dn.txt")

    with open(test_up_path, "w") as f:
        f.write("HOWDY\n")

    bucket = "media.angel-nft.com"
    channel = "tuttle_twins/manifests"

    s3_upload_text_file(up_path=test_up_path, bucket=bucket, channel=channel)

    up_file = os.path.basename(test_up_path)
    key = f"{channel}/{up_file}"
    s3_download_text_file(bucket=bucket, key=key, dn_path=test_dn_path)

    # deep comparison
    result = filecmp.cmp(test_up_path, test_dn_path, shallow=False)
    assert result == True, "ERROR: files are not the same"

    # cleanup
    os.remove(test_up_path)
    os.remove(test_dn_path)


def test_s3_list_files():
    '''
    test s3_list_files using hard-coded values
    NOTE: this will fail if the following S3 URI is not found 
    s3://media.angel-nft.com/tuttle_twins/manifests/S01E01-manifest-2022-05-02T12:43:24.662714.jl
    '''
    bucket = "media.angel-nft.com"
    dir = "tuttle_twins/manifests"
    prefix = "S01E01-manifest"
    suffix = ".jl"
    key_pattern = "2022-05-02"

    s3_key_rows = s3_list_files(bucket=bucket, dir=dir, prefix=prefix, suffix=suffix, key_pattern=key_pattern, verbose=True)
    assert len(s3_key_rows) > 0, "ERROR: s3_list_files returned zero s3_key"


def test_s3_ls_recursive():
    prefix = "tuttle_twins/ML/test/Common"
    episode_key_pattern = f"TT_S01_E01_FRM-.+\.jpg"
    s3_uri = f"s3://media.angel-nft.com/{prefix}/ | egrep \"{episode_key_pattern}\""
    s3_keys = s3_ls_recursive(s3_uri)
    assert len(s3_keys) > 0
    for s3_key in s3_keys:
        assert prefix in s3_key.get_key()

if __name__ == "__main__":

    # run tests if the only argv is this module name
    if len(sys.argv) == 1:
        test_s3_copy_file()
        test_s3_upload_download()
        test_s3_list_files()
        # test_s3_ls_recursive()
    
    # run s3_list_file_cli if any command line args are given
    else:
        s3_list_file_cli()
