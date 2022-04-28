import pandas as pd
import numpy as np
import gspread
import os
import time
import json

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

def count_lines(filename) :
    with open(filename, 'r') as fp:
        num_lines = sum(1 for line in fp)
    return num_lines

def process_episode(credentials_file, episode):

    share_link = episode["share_link"]
    manifest_file = episode["manifest_file"]
    spreadsheet_url = episode["spreadsheet_url"]

    gc = gspread.service_account(filename=credentials_file)
    gsheet = gc.open_by_url(share_link)
    data = gsheet.sheet1.get_all_records()

    df = pd.DataFrame(data)
    num_rows = df.shape[0]
    # df.info(verbose=True)
    # df.describe(include='all')

    print(f"input spread_sheet_url:{spreadsheet_url} num_rows:{num_rows}")

    thumbnails_base = df.columns[0]
    stamps_base = thumbnails_base.replace("thumbnails","stamps")
    df['src-ref'] = stamps_base + df["FRAME NUMBER"] + ".jpg"
    df['class'] = \
        np.where(df["JONNY's RECLASSIFICATION"].str.len() > 0, df["JONNY's RECLASSIFICATION"],
        np.where(df["SUPERVISED CLASSIFICATION"].str.len() > 0, df["SUPERVISED CLASSIFICATION"],
        np.where(df["UNSUPERVISED CLASSIFICATION"].str.len() > 0, df["UNSUPERVISED CLASSIFICATION"], None)))

    df = df[['src-ref','class']]

    epoch_time_in_seconds = time.time()
    tmpfile = f"tmpfile-{epoch_time_in_seconds}.jl"

    with open(tmpfile, "w") as f:
        print(df.to_json(orient='records', lines=True),file=f, flush=False)

    with open(manifest_file, "w") as w:  
        with open(tmpfile, "r") as r:
            lines = r.readlines()
            for line in lines:
                w.writelines([line.replace("\\/","/")])
    
    os.remove(tmpfile)

    num_lines = count_lines(manifest_file)
    print(f"output manifest_file:{manifest_file} num_lines:{num_lines}")


# pseudo-global variables
project_name = "My Project 46604"
project_id = "planar-outlook-348318"
project_number = "591253385669"
project_location = "angel.com"
service_account_email = "shawn-becker-pyspark@planar-outlook-348318.iam.gserviceaccount.com"
credentials_file = "/Users/shawnbecker-mbp/Google Drive/My Drive/Google Cloud Platform/gsheets-pyshark-348317-2b9d25a0fa1e.json"

# tt_s01_episodes = [
#    {
#        "season": "S01",
#        "episode": "E01",
#        "spreadsheet_title": "Tuttle Twins S01E01 Unsupervised Clustering", 
#        "spreadsheet_url": "https://docs.google.com/spreadsheets/d/1cr_rXVh0eZZ4aLtFKb5dw8jfBtksRezhY1X5ys6FckI/edit#gid=1690818184", 
#        "share_link": "https://docs.google.com/spreadsheets/d/1cr_rXVh0eZZ4aLtFKb5dw8jfBtksRezhY1X5ys6FckI/edit?usp=sharing",
#        "manifest_file": "S01E01-manifest.jl"
#    },
#    {
#        "season": "S01",
#        "episode": "E02",
#        "spreadsheet_title": "Tuttle Twins S01E02 Unsupervised Clustering",
#        "spreadsheet_url": "https://docs.google.com/spreadsheets/d/1v40TwUEphfX174xbAE-L3ORKqRz7S_jKeSeilibnkqQ/edit#gid=1690818184",
#        "share_link": "https://docs.google.com/spreadsheets/d/1v40TwUEphfX174xbAE-L3ORKqRz7S_jKeSeilibnkqQ/edit?usp=sharing",
#        "manifest_file": "S01E02-manifest.jl"
#    }
# ]

episodes_json_file = "tuttle-twins-S01-episodes.json"
with open(episodes_json_file,"r") as f:
    tt_s01_episodes = json.load(f)
    for episode in tt_s01_episodes:
        process_episode(credentials_file, episode)



