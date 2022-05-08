from typing import TypedDict
class ManifestRow(TypedDict):
   episode_id: str   # e.g. "S01E02"
   img_url: str      # e.g. non-editable "https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/TT_S01_E01_FRM-00-00-08-17.jpg"
   img_frame: str    # e.g. non-editable "TT_S01_E01_FRM-00-00-08-17"

   old_key: str      # current location if img_frame in s3, from old_class and old_folder
   old_class: str    # e.g. "Junk","Common","Uncommon","Rare", or "Legendary", from search 
   old_folder: str   # e.g. None, "train", "validate" or "Test", from search

   new_key: str      # future location of img_frame in s3 or None, from new_class and new_folder
   new_class: str    # future class, from google sheet or None
   new_folder: str   # future folder, random selection

   manifest_row_columns = \
      ['episode_id','img_url','img_frame', \
      'old_key','old_class','old_folder', \
      'new_key','new_class','new_folder]
