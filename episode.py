from typing import TypedDict, Tuple
import re

# the format of each "episode" in the season manifest file
class Episode(TypedDict):
    episode_id: str         # e.g. "S01E02"
    spreadsheet_title: str  # e.g. "Tuttle Twins S01E02 Unsupervised Clustering"
    spreadsheet_url: str    # e.g. "https://docs.google.com/spreadsheets/d/1v40TwUEphfX174xbAE-L3ORKqRz7S_jKeSeilibnkqQ/edit#gid=1690818184",
    share_link: str         # e.g. "https://docs.google.com/spreadsheets/d/1v40TwUEphfX174xbAE-L3ORKqRz7S_jKeSeilibnkqQ/edit?usp=sharing"

    # example episode_id: S01E11
    episode_id_regex_pattern = re.compile("^(S\d\d)(E\d\d)$")
    # episode_id_regex_pattern.search(episode_id).group() -> (season_code,. episode_cpde)
    
    # example episode_key: tuttle_twins/ML/validate/Rare/TT_S01_E01_FRM-00-00-09-01.jpg
    episode_key_regex_pattern = re.compile("^(\w+)/ML/(\w+)/(\w+)/(TT_S\d\d_E\d\d.+)\.jpg$")
    
    episode_key_folder_pattern = r"^\w+/ML/(\w+)/\w+/TT_S\d\d_E\d\d.+\.jpg$"
    episode_key_img_class_pattern = r"^\w+/ML/\w+/(\w+)/TT_S\d\d_E\d\d.+\.jpg$"
    episode_key_img_frame_pattern = r"^\w+/ML/\w+/\w+/(TT_S\d\d_E\d\d.+)\.jpg$"

    episode_key_columns = ['episode_id', 'last_modified', 'size', 'key', 'folder', 'img_class', 'img_frame']

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

    @staticmethod
    def parse_episode_key(episode_key: str) -> Tuple[str, str, str]:
        result = Episode.episode_key_regex_pattern.search(episode_key)
        folder = result.group(2)
        img_class = result.group(3)
        img_frame = result.group(4)
        return (folder, img_class, img_frame)

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

def test_parse_episode_key():
    episode_key = "tuttle_twins/ML/validate/Rare/TT_S01_E01_FRM-00-00-09-01.jpg"
    (folder, img_class, img_frame) = Episode.parse_episode_key(episode_key)
    assert folder == "validate", f"ERROR: expected folder: validate not {folder}"
    assert img_class == "Rare", f"ERROR: expected img_class: Rare not {img_class}"
    expected_img_frame = "TT_S01_E01_FRM-00-00-09-01"
    assert img_frame == expected_img_frame, "ERROR: expected img_frame: {expected_img_frame} not {img_Frame}"

def assert_name_value(name: str, result: str, expected: str) -> None:
    assert result == expected, f"ERROR: expected {name}: {expected} not {result}"

def test_parse_episode_key_parts():
    episode_key = "tuttle_twins/ML/validate/Rare/TT_S01_E01_FRM-00-00-09-01.jpg"

    groups = re.search(Episode.episode_key_folder_pattern, episode_key)
    assert_name_value(name="folder", result=groups(1), expected= "validate")
    
    groups = re.search(Episode.episode_key_img_class, episode_key)
    assert_name_value(name="img_class", result=groups(1), expected= "Rare")

    groups = re.search(Episode.episode_key_img_frame, episode_key)
    assert_name_value(name="img_frame", result=groups(1), expected= "TT_S01_E01_FRM-00-00-09-01")

    
if __name__ == '__main__':
    test_parse_episode_id()
    test_split_episode_id()
    test_parse_episode_key()
    test_parse_episode_key_parts()
