from typing import TypedDict, Tuple
import re

# the format of each "episode" dict in the season manifest file 
# e.g. s3://media.angel-nft.com/tuttle_twins/manifests/S01-season.json
class Episode(TypedDict):
    episode_id: str                     # e.g. "S01E02"
    google_spreadsheet_title: str       # e.g. "Tuttle Twins S01E02 Unsupervised Clustering"
    google_spreadsheet_url: str         # e.g. "https://docs.google.com/spreadsheets/d/1v40TwUEphfX174xbAE-L3ORKqRz7S_jKeSeilibnkqQ/edit#gid=1690818184",
    google_spreadsheet_share_link: str  # e.g. "https://docs.google.com/spreadsheets/d/1v40TwUEphfX174xbAE-L3ORKqRz7S_jKeSeilibnkqQ/edit?usp=sharing"

    # example episode_id: S01E11
    episode_id_regex_pattern = re.compile("^(S\d\d)(E\d\d)$")
    # episode_id_regex_pattern.search(episode_id).group() -> (season_code,. episode_cpde)
    
    @staticmethod
    def parse_episode_id(episode_id: str) -> Tuple[str,str]:
        result = Episode.episode_id_regex_pattern.search(episode_id)
        season_code = result.group(1)
        episode_code = result.group(2)
        return (season_code, episode_code)

    @staticmethod
    def split_episode_id(episode_id: str) -> str:
        (season_code, episode_code) = Episode.parse_episode_id(episode_id)
        return f"{season_code}_{episode_code}"

########################
# TESTS
########################

def test_parse_episode_id():
    episode_id = "S01E11"
    (season_code, episode_code) = Episode.parse_episode_id(episode_id)
    assert season_code == "S01", f"ERROR: expected season_code: S01 not {season_code}"
    assert episode_code == "E11", f"ERROR: expected episode_code: E11 not {episode_code}"
    
def test_split_episode_id():
    episode_id = "S01E11"
    split_code = Episode.split_episode_id(episode_id)
    expected = ""
    assert split_code == "S01_E11", f"ERROR: expected S01_E11: not {split_code}"
    
if __name__ == '__main__':
    test_parse_episode_id()
    test_split_episode_id()
