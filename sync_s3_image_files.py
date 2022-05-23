import json

from episode_service import get_file_names_from_all_stage_data_files
from s3_utils import s3_sync_download_files
from env import S3_MEDIA_ANGEL_NFT_BUCKET, LOCAL_SOURCE_IMAGES_DIR

# example:
#   activate
#   python sync_s3_image_files.py
#
def sync_s3_data_files():
    # get a list of all required file_names 
    file_names = get_file_names_from_all_stage_data_files()
    
    # convert file_names to src_keys
    # s3://media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/TT_S01_E01_FRM-00-00-00-00.jpg
    # src_bucket = media.angel-nft.com
    # src_key = tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/TT_S01_E01_FRM-00-00-00-00.jpg
    # file_name = TT_S01_E01_FRM-00-00-00-00
    # season_code = S01
    # episode_code = E01
    
    src_keys = []
    for file_name in file_names:
        parts = file_name.split("_")
        season_code_low = parts[1].lower()
        episode_code_low = parts[2].lower()
        src_prefix = f"tuttle_twins/{season_code_low}{episode_code_low}/default_eng/v1/frames/thumbnails/"
        src_key = src_prefix + file_name
        src_keys.append(src_key)
    
    result = s3_sync_download_files(
        src_bucket=S3_MEDIA_ANGEL_NFT_BUCKET, 
        src_keys=src_keys, 
        dst_folder=LOCAL_SOURCE_IMAGES_DIR)
    
    print("sync_s3_data_files results:", json.dumps(result, indent=4))


if __name__ == "__main__":
    sync_s3_data_files()
