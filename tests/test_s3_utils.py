# call from project directory
# python -m unittest tests/test_s3_utils

import unittest

from s3_utils import *
from file_utils import generate_big_random_bin_file, compare_big_bin_files

import logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger("test_s3_utils")

class TestS3UtilMethods(unittest.TestCase):
    
        
    def test_s3_copy_file(self):
        '''
        "src_url": "s3://media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/TT_S01_E01_FRM-00-00-00-03.jpg", 
        "src_key": "tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/TT_S01_E01_FRM-00-00-00-03.jpg", 
        "dst_key": "tuttle_twins/ML/deleteme/test.jpg"
        '''
        src_bucket = "media.angel-nft.com"
        src_key = "tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/TT_S01_E01_FRM-00-00-00-03.jpg"
        dst_bucket = "media.angel-nft.com"
        dst_key = "tuttle_twins/ML/deleteme/test.jpg"

        logger.debug(f"test_s3_copy_file src_bucket: {src_bucket}")
        logger.debug(f"test_s3_copy_file src_key: {src_key}")
        logger.debug(f"test_s3_copy_file dst_bucket: {dst_bucket}")
        logger.debug(f"test_s3_copy_file dst_key: {dst_key}")

        response = s3_copy_file(src_bucket, src_key, dst_bucket, dst_key)
        self.assertTrue('ResponseMetadata' in response, f"ERROR: no ResponseMetaData key")
        self.assertTrue('HTTPStatusCode' in response['ResponseMetadata'], f"ERROR: no HTTPStatusCode key")
        httpStatusCode = response['ResponseMetadata']['HTTPStatusCode']
        self.assertTrue(httpStatusCode == 200, f"ERROR: bad httpStatusCode: {httpStatusCode}")

        s3_delete_file(dst_bucket, dst_key)
        logger.debug(f"test_s3_copy_file passed")

    def test_s3_upload_download_1Mbyte_binary_file(self):
        Mbytes = 1
        bytes = round(Mbytes * 1024 * 1024)
        test_up_filename = f"test-up-file-{round(time() * 1000)}"
        test_dn_filename = f"test-dn-file-{round(time() * 1000)}"
        test_up_file = "/tmp/" + test_up_filename
        test_dn_file = "/tmp/" + test_dn_filename

        logger.debug(f"test_s3_upload_download_1Mbyte_binary_file test_up_file:{test_up_file}")
        generate_big_random_bin_file(filename=test_up_file, size=bytes)

        bucket = "media.angel-nft.com"
        channel = "tuttle_twins/manifests"

        logger.debug(f"test_s3_upload_download_1Mbyte_binary_file s3_upload_file() starts")
        s3_upload_file(up_path=test_up_file, bucket=bucket, channel=channel)
        logger.debug(f"test_s3_upload_download_1Mbyte_binary_file s3_upload_file() finished")

        key = f"{channel}/{test_up_filename}"

        logger.debug(f"test_s3_upload_download_1Mbyte_binary_file test_dn_file:{test_dn_file}")
        logger.debug(f"test_s3_upload_download_1Mbyte_binary_file s3_download_file() starts")
        s3_download_file(bucket=bucket, key=key, dn_path=test_dn_file)
        logger.debug(f"test_s3_upload_download_1Mbyte_binary_file s3_download_file() finished")

        s3_delete_file(bucket=bucket, key=key)

        logger.debug(f"test_s3_upload_download_1Mbyte_binary_file compare_big_bin_files() starts")
        self.assertTrue(compare_big_bin_files(test_up_file, test_dn_file))
        logger.debug(f"test_s3_upload_download_1Mbyte_binary_file compare_big_bin_files() finished")

        os.remove(test_up_file)
        os.remove(test_dn_file)


    def test_s3_list_files(self):
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

        logger.debug(f"test_s3_list_files s3_list_files() starts")
        s3_key_rows = s3_list_files(bucket=bucket, dir=dir, prefix=prefix, suffix=suffix, key_pattern=key_pattern, verbose=True)
        self.assertTrue(len(s3_key_rows) > 0, "ERROR: s3_list_files returned zero S3Key")
        logger.debug(f"test_s3_list_files s3_list_files() finished")

    def test_s3_ls_recursive(self):
        prefix = "tuttle_twins/ML"
        episode_key_pattern = f"train/Uncommon/TT_S01_E01_FRM-.+\.jpg"
        s3_uri = f"s3://media.angel-nft.com/{prefix}/ | egrep -e \"{episode_key_pattern}\""

        logger.info(f"test_s3_ls_recursive s3_ls_recursive() starts")
        keys = s3_ls_recursive(s3_uri)
        self.assertTrue(len(keys) > 0, "ERROR: s3_ls_recursive return zero keys")
        logger.info(f"test_s3_ls_recursive s3_ls_recursive() finished")

        for key in keys:
            self.assertTrue(prefix in key.get_key(), "ERROR: prefix not found in key.get_key()")

    def test_s3_list_file_cli(self):
        argv = ["s3_utils.py","media.angel-nft.com", "tuttle_twins/manifests", "--suffix", ".jl" ]
        logger.debug(f"test_s3_list_file_cli s3_list_file_cli() starts")
        s3_keys = s3_list_file_cli(argv)
        self.assertTrue(len(s3_keys) > 0)
        logger.debug(f"test_s3_list_file_cli s3_list_file_cli() finished")


if __name__ == '__main__':
    unittest.main()
