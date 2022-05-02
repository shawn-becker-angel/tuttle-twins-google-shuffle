import pandas as pd
import numpy as np
import gspread
import os
import time
import json
from datetime import datetime

manifest_directory_path = "manifests"

# Create a new Google Cloud project
# Go to https://console.developers.google.com/
# Click "create a new project" link named "gsheets-pyshark"
# Click on top right bell icon to see a notification that the project has been created
# Click "View" to view the detail page of the newly created project
# Save your <project_service_account email> 

# Create Keys for your new Google Cloud Project
# Click the "Keys" tab, click "Add Key", select "Create New Key", select Key type "JSON", click "CREATE" link, 
# Copy the new JSON credentials file from your Downloads folder to a safe location
# Set credentials_file variable to this location

# Open a pre-existing Google Drive spreadsheet
# Go to url "https://docs.google.com/spreadsheets/d/1cr_rXVh0eZZ4aLtFKb5dw8jfBtksRezhY1X5ys6FckI/edit#gid=1690818184"
# Ensure that the first row is column names

# Or create a new Google Drive spreadsheet
# Go to https://docs.google.com/spreadsheets/u/0/?tgif=d and click "Blank" to create a new spreadsheet
# Rename the newly created sheet to "pyshark tutorial (https://pyshark.com/google-sheets-api-using-python)"
# Fill out the new spreadsheet - first row is column names

# Share the spreadsheet with <project_service_account_email>
# Click "Share" button at top right
# Under "Share with people and groups" add your <project_service_account_email> from above
# Alert window says "You are sharing to <project_service_account_email> who is not in the Google Workspace organization that this item belongs to."
# Click "Share anyway" link
# Click "Copy link" and set share_link variable to this url 

# Create the utc_datetime_iso manifest file
# Upload it to s3://bucket/tuttle-twins-season-episode-manifests/

def count_lines(filename) :
    with open(filename, 'r') as fp:
        num_lines = sum(1 for line in fp)
    return num_lines

def create_episode_manifest_file(google_credentials_file, episode):
    '''
    read the google sheet for this episode into a df
    compute and keep only columns 'src-ref' and 'class'
    save this episode's df as a manifest.jl file 
    '''

    # get attributes from episode object
    share_link = episode["share_link"]
    manifest_file = episode["manifest_file"]
    spreadsheet_url = episode["spreadsheet_url"]

    # verify manifest file has '.jl' json lines extension
    if not manifest_file.endswith(".jl"):
        raise Exception("episode manifest_file:" + manifest_file + " requires '.jl' json lines extension")

    # verify manifest file has substring "<utc_datetime_iso>"
    if manifest_file.find("<utc_datetime_iso>") == -1:
        raise Exception("episode manifest_file:" + manifest_file + " requires replaceable <utc_datetime_iso> substring")

    # replace <utc_datetime_iso> with the current value, e.g. '2022-04-28T10:43:48.733843'
    utc_datetime_iso = datetime.now().isoformat()
    manifest_file = manifest_file.replace("<utc_datetime_iso>", utc_datetime_iso)

    # use the google credentials file and the episode's share_link to read
    # the raw contents of the first sheet into df
    gc = gspread.service_account(filename=google_credentials_file)
    gsheet = gc.open_by_url(share_link)
    data = gsheet.sheet1.get_all_records()
    df = pd.DataFrame(data)

    num_rows = df.shape[0]
    # df.info(verbose=True)
    # df.describe(include='all')
    print(f"input spread_sheet_url:{spreadsheet_url} num_rows:{num_rows}")

    # fetch the 's3_thumbnails_base_url' from the name of column zero, e.g.
    #   https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/
    s3_thumbnails_base_url = df.columns[0]

    # verify that s3_thumbnails_base_url contains 'episode_base_code', e.g. 
    #   "s01e01"
    episode_base_code = episode["season"].lower() + episode["episode"].lower()
    if s3_thumbnails_base_url.find(episode_base_code) == -1:
        raise Exception(f"s3_thumbnails_base_url fails to include 'episode_base_code': {episode_base_code}")

    # convert the s3_thumbnails_base_url into the s3_stamps_base_url
    s3_stamps_base_url = s3_thumbnails_base_url.replace("thumbnails","stamps")  

    # verify that all rows of the "FRAME NUMBER" column contain 'episode_frame_code', e.g. 
    #   "TT_S01_E01_FRM"  
    # example FRAME_NUMBER column: 
    #   "TT_S01_E01_FRM-00-00-08-11"
    episode_frame_code = "TT_" + episode["season"].upper() + "_" + episode["episode"].upper() + "_FRM"

    # verify that "FRAME NUMBER" column of all rows contain 'episode_frame_code'
    matches = df[df['FRAME NUMBER'].str.contains(episode_frame_code, case=False)]
    failure_count = len(df) - len(matches)
    if failure_count > 0:
        raise Exception(f"{failure_count} rows have FRAME NUMBER values that fail to include 'episode_frame_code': {episode_frame_code}" )

    # compute the "src-ref" column of each row using the s3_stamps_base_url and the "FRAME_NUMBER" of that row
    df['src-ref'] = s3_stamps_base_url + df["FRAME NUMBER"] + ".jpg"

    # compute the "class" column of each row as the first available "CLASSIFICATION" for that row or None
    df['class'] = \
        np.where(df["JONNY's RECLASSIFICATION"].str.len() > 0, df["JONNY's RECLASSIFICATION"],
        np.where(df["SUPERVISED CLASSIFICATION"].str.len() > 0, df["SUPERVISED CLASSIFICATION"],
        np.where(df["UNSUPERVISED CLASSIFICATION"].str.len() > 0, df["UNSUPERVISED CLASSIFICATION"], None)))

    # drop all columns except these 
    df = df[['src-ref','class']]

    # convert df to a list of dicts, one for each row
    df_list_of_row_dicts = df.to_dict('records')

    # write all rows of manifest_file to a json lines file under manifest_directory_path
    if not os.path.exists(manifest_directory_path):
        os.makedirs(manifest_directory_path)

    manifest_path = f"{manifest_directory_path}/{manifest_file}"
    # write each row_dist to the manifest_file as a flat row_json_str
    with open(manifest_path, "w") as w: 
        for row_dict in df_list_of_row_dicts:
            row_json_str = json.dumps(row_dict)
            # row_json_str = row_json_str.replace("\\/","/")
            w.writelines(row_json_str)
    
    num_lines = count_lines(manifest_path)
    print(f"output episode manifest_path:{manifest_path} num_lines:{num_lines}")

# This JSON file is created manually by members of the Angel Studios Data team.
# See the README file for instructions
google_credentials_file = "./credentials/gsheets-pyshark-348317-2b9d25a0fa1e.json"

# Each season_episode_file, e.g. "S01-episodes.json" describes the parameters 
# used to create episode_manifest files for all of its episodes.

# These JSON files are created manually by members of the Angel Studios Data team
season_episode_files = [
    "season_episodes/S01-episodes.json"
]

# create an episode manifest file for all episodes in all seasons
for season_episode_file in season_episode_files:
    with open(season_episode_file,"r") as f:
        season_episodes = json.load(f)
        for episode in season_episodes:
            create_episode_manifest_file(google_credentials_file, episode)



