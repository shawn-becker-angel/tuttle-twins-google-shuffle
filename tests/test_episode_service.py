# call from project directory
# python -m unittest tests/test_episode_service.py

import unittest

from episode_service import *

class TestEpisodeServiceMethods(unittest.TestCase):

    def get_test_episode(self):
        episode_dict = {
            "season_code": "S01",
            "episode_code": "E02",
            "google_spreadsheet_title": "Tuttle Twins S01E02 Unsupervised Clustering",
            "google_spreadsheet_url": "https://docs.google.com/spreadsheets/d/1v40TwUEphfX174xbAE-L3ORKqRz7S_jKeSeilibnkqQ/edit#gid=1690818184",
            "google_spreadsheet_share_link": "https://docs.google.com/spreadsheets/d/1v40TwUEphfX174xbAE-L3ORKqRz7S_jKeSeilibnkqQ/edit?usp=sharing"
        }
        episode = Episode(episode_dict)
        return episode

    def test_find_sampled_google_episode_keys_df(self):
        episode = self.get_test_episode()
        episode_id = episode.get_episode_id()
        G = find_sampled_google_episode_keys_df(episode)
        if len(G) > 0:
            expected = set(['episode_id', 'img_src', 'img_frame', 'new_ml_key'])
            result = set(G.columns)
            self.assertEqual(result,expected, f"ERROR: expected G.columns: {expected} not {result}")

    # def test_s3_find_episode_jpg_keys_df(self):
    #     episode = self.get_test_episode()
    #     episode_id = episode.get_episode_id()
    #     C = s3_find_episode_jpg_keys_df(episode)
    #     if len(C) > 0:
    #         expected = set(C[['episode_id', 'img_frame', 'ml_key']])
    #         result = set(C.columns)
    #         self.assertEqual(result, expected, f"ERROR: expected C.columns: {expected} not {result}")


    # def test_create_google_episode_stage_data_files(self):
    #     episode = self.get_test_episode()
    #     episode_stage_data_files = create_google_episode_stage_data_files(episode=episode)
    #     for stage in DATA_STAGES:
    #         self.assertIsNotNone(episode_stage_data_files[stage])


    # def test_create_all_stage_data_files(self):
    #     result = create_all_stage_data_files()
    #     self.assertEqual(len(result), len(DATA_STAGES))


if __name__ == '__main__':
    unittest.main()
