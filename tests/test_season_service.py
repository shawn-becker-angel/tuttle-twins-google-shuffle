# call from project directory
# python -m unittest tests/test_season_service.py

import unittest

from season_service import *

class TestSeasonServiceMethods(unittest.TestCase):

    def test_find_all_season_s3_keys(self):
        all_season_s3_keys = find_all_season_manifest_s3_keys()
        self.assertTrue(len(all_season_s3_keys) > 0, "ERROR: no session keys found")

    def test_find_all_season_codes(self):
        all_season_codes = find_all_season_codes()
        self.assertTrue(len(all_season_codes) > 0, "ERROR: no season codes found")

    def test_download_all_seasons_episodes(self):
        all_season_episodes = download_all_seasons_episodes()
        self.assertTrue(len(all_season_episodes) > 0, "ERROR: no all_season_episodes found")

if __name__ == "__main__":
     unittest.main()
