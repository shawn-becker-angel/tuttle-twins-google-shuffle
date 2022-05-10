from episode import Episode
from s3_key import s3_key
import s3_utils
import datetime

from typing import List

from constants import S3_MEDIA_ANGEL_NFT_BUCKET, S3_MANIFESTS_DIR

# use pip install python-dotenv
from dotenv import load_dotenv
load_dotenv()


# ============================================
# season_service MODULE OVERVIEW
#

def find_all_season_keys() -> List[s3_key]:
    '''
    Use s3_utils.s3_list_files to find
    the keys of all season json files found
    under the s3 manifests directory
    '''
    season_keys = s3_utils.s3_list_files(
        bucket=S3_MEDIA_ANGEL_NFT_BUCKET, 
        dir=S3_MANIFESTS_DIR, 
        suffix="-season.json")
    return season_keys

def download_season_episodes(season_key: str) -> List[Episode]:
    '''
    Download a season json file from the s3 manifests directory
    and return a list of all Episode dicts in that season json 
    file. Return empty list if season json is not found
    '''
    tmp_file = "/tmp/tmp-" + datetime.datetime.utcnow().isoformat()
    try:
        s3_utils.download_text_file(
            bucket=S3_MEDIA_ANGEL_NFT_BUCKET,
            key=season_key,
            dn_file=tmp_file)

        episodes = []

        if os.path.exists(tmp_file):
            json_dicts = json.load(tmp_file)
            for json_dict in json_dicts:
                episode = Episode(json_dict)
                episodes.append(episode)

    finally:
        if os.path.exists(tmp_file):
            os.remove(tmp_file)
        return episodes

def download_all_season_episodes() -> List[Episode]:
    '''
    Return a list of all Episodes from all season json
    files found in the S3 manifest directory. Return an empty 
    list if season json files with Episode dicts are not found.
    '''
    all_episodes = []
    season_keys = Season.find_season_keys()
    for season_key in season_keys:
        season_episodes = download_season_episodes(season_key)
        if season_episodes and len(season_episodes) > 0:
            for season_episode in season_episodes:
                episode = Episode(season_episode)
                all_episodes.extend(episode)
    return all_episodes


# =============================================
# TESTS
# =============================================

def test_find_all_season_keys():
    all_season_keys = find_all_season_keys()
    assert len(all_season_keys) > 0, "ERROR: no session keys found"

def test_download_all_season_episodes():
    all_season_episodes = download_all_season_episodes()
    assert len(all_season_episodes) > 0, "ERROR: no all_season_episodes found"

if __name__ == "__main__":
    test_find_all_season_keys()

