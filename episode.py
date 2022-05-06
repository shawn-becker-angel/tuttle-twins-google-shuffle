from typing import TypedDict, Tuple
import re

# the format of each "episode" in the season manifest file
class Episode(TypedDict):
    episode_id: str         # e.g. "S01E02"
    spreadsheet_title: str  # e.g. "Tuttle Twins S01E02 Unsupervised Clustering"
    spreadsheet_url: str    # e.g. "https://docs.google.com/spreadsheets/d/1v40TwUEphfX174xbAE-L3ORKqRz7S_jKeSeilibnkqQ/edit#gid=1690818184",
    share_link: str         # e.g. "https://docs.google.com/spreadsheets/d/1v40TwUEphfX174xbAE-L3ORKqRz7S_jKeSeilibnkqQ/edit?usp=sharing"

    @staticmethod
    def parse_codes(episode_id: str) -> Tuple[str,str]:
        episode_id_pattern = "(S\d\d)(E\d\d)"
        result = re.search(episode_id_pattern, episode_id)
        season_code = result.group(1)
        episode_code = result.group(2)
        return (season_code, episode_code)

########################
# TESTS
########################

    @staticmethod
    def test_parse_codes():
        episode_id = "S01E11"
        (season_code, episode_code) = Episode.parse_codes(episode_id)
        assert season_code == "S01", f"expected season_code: 01 not {season_code}"
        assert episode_code == "E11", f"expected episode_code: 01 not {episode_code}"

if __name__ == '__main__':
    Episode.test_parse_codes()