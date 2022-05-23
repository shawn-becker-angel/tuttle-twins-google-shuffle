# call from project directory
# python -m unittest tests/test_s3_key.py

import unittest

from s3_key import *

class TestS3KeyMethods(unittest.TestCase):

    def test_multi_lines(self):
        s3_ls_lines = []
        s3_ls_lines.append("2022-05-07 02:16:43       2632 tuttle_twins/ML/test/Common/TT_S01_E01_FRM-00-00-13-01.jpg\n")
        s3_ls_lines.append("2022-05-07 02:16:43       2703 tuttle_twins/ML/test/Common/TT_S01_E01_FRM-00-00-13-04.jpg\n")
        s3_ls_lines.append("2022-05-07 02:16:43       2748 tuttle_twins/ML/test/Common/TT_S01_E01_FRM-00-00-13-22.jpg\n")
                        
        s3keys = []
        for s3_ls_line in s3_ls_lines:
            s3key = S3Key(s3_ls_line=s3_ls_line)
            s3keys.append(s3key)
        expected = 3
        result = len(s3keys)
        self.assertEqual(result, expected, f"ERROR: expected num s3keys: {expected} not {result}")

        cnt = 0
        prefix = "tuttle_twins/ML/test/Common/TT_S01_E01_FRM"
        for s3key in s3keys:
            self.assertTrue(prefix in s3key.get_key(), "ERROR: prefix not found in key.get_key()")
            cnt += 1
        logger.debug(f"s3key.get_key() tested {cnt} s3keys")

        # how to access a staticmethod within class
        # see https://stackoverflow.com/a/12718272/18218031
        dicts = get_S3Key_dict_list.__func__(s3keys)
        cnt = 0
        for d in dicts:
            self.assertTrue(prefix in d['key'], "ERROR: prefix not found in d['key']")
            cnt += 1
        logger.debug(f"get_S3Key_dict_list() tested {cnt} s3keys")

if __name__ == '__main__':
    unittest.main()
    
