from episode import Episode
from s3_key import s3_key
import s3_utils
import datetime
import os
import json

from typing import List

from env import S3_MEDIA_ANGEL_NFT_BUCKET, S3_MANIFESTS_DIR

# use pip install python-dotenv
from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger("season")

# ============================================
# season_service MODULE OVERVIEW
#

def find_all_season_manifest_s3_keys() -> List[s3_key]:
    '''
    Use s3_utils.s3_list_files to find
    the keys of all season json files found
    under the s3 manifests directory
    e.g. S01-episodes.json
    '''
    season_manifest_s3_keys = s3_utils.s3_list_files(
        bucket=S3_MEDIA_ANGEL_NFT_BUCKET, 
        dir=S3_MANIFESTS_DIR, 
        suffix="-episodes.json")
    return season_manifest_s3_keys

def download_season_episodes(season_manifest_key: str) -> List[Episode]:
    '''
    Download a season json file from the s3 manifests directory
    and return a list of all Episode dicts in that season json 
    file. Return empty list if season json is not found
    '''
    assert season_manifest_key is not None, "undefined season_manifest_key"
    episodes = []
    tmp_file = "/tmp/tmp-" + datetime.datetime.utcnow().isoformat() + ".json"
    try:
        s3_utils.s3_download_text_file(
            bucket=S3_MEDIA_ANGEL_NFT_BUCKET,
            key=season_manifest_key,
            dn_path=tmp_file)

        if os.path.exists(tmp_file):
            with open(tmp_file,"r") as f:
                json_dicts = json.load(f)
                for json_dict in json_dicts:
                    episode = Episode(json_dict)
                    episodes.append(episode)
                
    except Exception as exp:
        logger.error(type(exp), str(exp))
        raise

    finally:
        if os.path.exists(tmp_file):
            os.remove(tmp_file)
        return episodes

def download_all_seasons_episodes() -> List[Episode]:
    '''
    Return a list of all Episodes from all season json
    files (e.g. S01-season.json) found in the S3 manifest directory. Return an empty 
    list if season json files with Episode dicts are not found.
    '''
    all_episodes = []
    try:
        season_manifest_s3_keys = find_all_season_manifest_s3_keys()
        for season_manifest_s3_key in season_manifest_s3_keys:
            season_episodes = download_season_episodes(season_manifest_s3_key['key'])
            if season_episodes and len(season_episodes) > 0:
                all_episodes.extend(season_episodes)
    except Exception as exp:
        logger.error(type(exp),str(exp))
        raise
    return all_episodes


# =============================================
# TESTS
# =============================================

def test_find_all_season_s3_keys():
    all_season_s3_keys = find_all_season_manifest_s3_keys()
    assert len(all_season_s3_keys) > 0, "ERROR: no session keys found"

def test_download_all_seasons_episodes():
    all_season_episodes = download_all_seasons_episodes()
    assert len(all_season_episodes) > 0, "ERROR: no all_season_episodes found"

if __name__ == "__main__":
    test_find_all_season_s3_keys()
    test_download_all_seasons_episodes()
    logger.info("done")
    

