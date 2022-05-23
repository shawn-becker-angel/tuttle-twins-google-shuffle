import re

import logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger("episode")


# This class contains the contents of each "episode" dict in the season manifest json file, 
# e.g. s3://media.angel-nft.com/tuttle_twins/manifests/S01-season.json
class Episode:
    season_code: str                    # e.g. "S01"
    episode_code: str                   # e.g. "E02"
    google_spreadsheet_title: str       # e.g. "Tuttle Twins S01E02 Unsupervised Clustering"
    google_spreadsheet_url: str         # e.g. "https://docs.google.com/google_spreadsheets/d/1v40TwUEphfX174xbAE-L3ORKqRz7S_jKeSeilibnkqQ/edit#gid=1690818184",
    google_spreadsheet_share_link: str  # e.g. "https://docs.google.com/google_spreadsheets/d/1v40TwUEphfX174xbAE-L3ORKqRz7S_jKeSeilibnkqQ/edit?usp=sharing"
    episode_id: str

    def __init__(self, input_dict: dict):
        try:
            self.season_code = self.get_dict_value(input_dict, 'season_code')
            self.episode_code = self.get_dict_value(input_dict,'episode_code')
            self.google_spreadsheet_title = self.get_dict_value(input_dict,'google_spreadsheet_title')
            self.google_spreadsheet_url = self.get_dict_value(input_dict,'google_spreadsheet_url')
            self.google_spreadsheet_share_link = self.get_dict_value(input_dict,'google_spreadsheet_share_link')
            self.episode_id = self.season_code + self.episode_code
        except Exception as exp:
            log.error(type(exp), str(exp))
            raise
    
    def get_dict_value(self, input_dict: dict, key: str) -> str:
        if key not in input_dict.keys():
            raise KeyError(f"ERROR: input_dict lacks key:{key}")
        return input_dict[key]

    def get_episode_id(self):
        return self.episode_id

    def get_split_episode_id(self):
        return self.season_code + '_' + self.episode_code
    
    def get_season_code(self):
        return self.season_code
    
    def get_episode_code(self):
        return self.episode_code
    
    def get_google_spreadsheet_title(self):
        return self.google_spreadsheet_title
    
    def get_google_spreadsheet_url(self):
        return self.google_spreadsheet_urld
    
    def get_google_spreadsheet_share_link(self):
        return self.google_spreadsheet_share_link
    
    def as_dict(self):
        output_dict = {}
        output_dict['season_code'] = self.season_code
        output_dict['episode_code'] = self.episode_code
        output_dict['google_spreadsheet_title'] = self.google_spreadsheet_title
        output_dict['google_spreadsheet_url'] = self.google_spreadsheet_url
        output_dict['google_spreadsheet_share_link'] = self.google_spreadsheet_share_link
        return output_dict
    
    def as_str(self):
        return str(self.as_dict())

########################
# TESTS
########################

def test_constructor():
    import json
    episode_dict = {
        "season_code": "S01",
        "episode_code": "E02",
        "google_spreadsheet_title":  "Tuttle Twins S01E02 Unsupervised Clustering",
        "google_spreadsheet_url": "https://docs.google.com/google_spreadsheets/d/1v40TwUEphfX174xbAE-L3ORKqRz7S_jKeSeilibnkqQ/edit#gid=1690818184",
        "google_spreadsheet_share_link": "https://docs.google.com/google_spreadsheets/d/1v40TwUEphfX174xbAE-L3ORKqRz7S_jKeSeilibnkqQ/edit?usp=sharing"
    }
    episode = Episode(episode_dict)
    assert episode.get_episode_id() == episode_dict['season_code'] + episode_dict['episode_code']
    logger.debug("new episode:" + json.dumps(episode.as_dict(),indent=4))

    
if __name__ == '__main__':
    from logger_utils import set_all_info_loggers_to_debug_level
    set_all_info_loggers_to_debug_level()
    test_constructor()
    logger.debug("done")
