from typing import TypedDict

# the format of each "episode" in the season manifest file
class Season(TypedDict):
    season_code: str         # e.g. "S01"

    # def get_key(self) -> str:
    #     return f"tuttle_twins/manifests/{self.season_code}-season.json"

