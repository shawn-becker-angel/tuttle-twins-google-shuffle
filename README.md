# The Tuttle Twins Rarity Estimation Project - Data Preparation    
## Background:  
The Angel Studios apps and website (https://www.angel.com) are introducing new functionality that allows users to purchase a "Non-Fungible Token" (NFT) for selected image frames from the productions that they host.  

This project aims to use machine learning technologies to create a predictive model that will be used to automatically set the starting NFT sell price for a given frame of the 'Tuttle Twins' production. 

Initial prices will be determined according to some measure of its "rarity". Rarity levels range from the highest level of "Legendary" down to lower levels of "Rare", "Uncommon", "Common", and "Trash".  

## Data Preparation  
This project uses "image-classification" algorithms with "convolutional neural networks" (CNNs) to train a model that can be used to estimate the level of rarity for a given image frame. This requires first creating a large set (thousands or millions) of frames that have been manually assigned a "rarity" level. Multiple rarity assignments are collected from both automated and manual sources. Ultimately, only the most trusted assignment is used as the supervised rarity classification for each image frame in the training set.

The CCN training process requires working with a random sampled sub-set of these pre-classified source image files.  

There are a lot of image files to manage for each episode. At a framerate of 24 frames per second x 60 seconds per minute x about 20 minutes per episode, this results in about 28,800 frames per episode. The huge effort of managing supervised classifications for all of these images is managed using spreadsheets stored in Google Drive.

These spreadsheets, which are organized by season and episode, are dynamic and will change without notice at any time. Images classifications and image files will change and new seasons and episodes may be added without explicit notification.  

### JPG image files:  
Pre-sized jpg source image files are stored in S3 under bucket `media.angel-nft.com` with directory structure `tuttle_twins/<season_code><episode_code>/default_eng/v1/frames/<img_size>`

Data preparation for training the CNN requires that the three target datasets contain only their required jpg files. Image files for these destination datasets are stored in S3 bucket `media.angel-nft.com` but with directory structure `tuttle_twins/ML/<dataset>/<classification>`.

## Random image assignment improves model quality:  
The Tuttle Twins image classification CNN model is trained to work over multiple seasons and episodes. Random placement of images among the datasets improves the quality of the predictive model. So the source jpg files used to train, validate and test all episodes need to be placed within one of the three destination datasets.  

## Sync rather than full-load strategy:  
Rather than deleting all datasets and then copying over all source jpg files from scratch every time any of the google spreadsheets change, we take pains to calculate the minimal S3 file copies and deletes needed. These costly S3 operations are used to bring the destination dataset files into sync with the target configuration defined among the google episode spreadsheets.

## One Google spreadsheet for each episode in a season:  
Each episode spreadsheet has the following columns:  
  * `S3_BASE_URI`: example:  
    `media.angel-nft.com/tuttle_twins/default_eng/v1/frames/thumbnails/`
  * `ROW_INDEX`: starts with 0  
  * `FRAME NUMBER`: example: `TT_S01_E01_FRM-00-00-08-12`  
  * `UNSUPERVISED CLASSIFICATION`: see `RARITY_CHOICES`  
  * `SUPERVISED CLASSIFICATION`: see `RARITY_CHOICES`  
  * `JONNY's RECLASSIFICATION`: see `RARITY_CHOICES`  
  * `METADATA`: example `[Emily, Space Tunnel, Time Machine]`   

### Authorize API access to the Google spreadsheets:    
In order to automatically process the Google spreadsheets, the application needs to obtain the credentials needed to access the Google Drive API. To do this, first go to `https://console.developers.google.com/`.  
Click "create a new project" link named "gsheets-pyshark".  
Click on top right bell icon to see a notification that the project has been created.  
Click "View" to view the detail page of the newly created project.    
Click to save your `project_service_account email`.   

### Create an API access key for the new Google Cloud Project  
Click the "Keys" tab, click "Add Key", select "Create New Key", select Key type "JSON", click "CREATE" link.  
Copy the new JSON credentials file from your Downloads folder to a safe location.  
Copy the google credientials file to the base directory of your local copy of this repo.  
Update the `GOOGLE_DRIVE_CREDENTIALS_FILE` property in the project's `.env` file accordingly.  
Make sure that the `.gitignore` file is configured to NOT upload this file to github.  
Note: if your credentials file does get pushed up to the github repo, you will receive a warning email from Google advising you that storing their credentials in github is against your signed Google Developer Agreement.  

### Open an existing spreadsheet and create its "Share Link"
Open an episode spreadsheet, like season-01, episode-01, at url `https://docs.google.com/google_spreadsheets/d/1cr_rXVh0eZZ4aLtFKb5dw8jfBtksRezhY1X5ys6FckI/edit#gid=1690818184`  
Ensure that the first row contains the standard column names.  
Click "Share" button at top right.  
Under "Share with people and groups" add your `project_service_account_email` that you used above.  
Alert window says "You are sharing to `project_service_account_email` who is not in the Google Workspace organization that this item belongs to."  
Click the "Share anyway" link.  
Click "Copy link" and save the "GOOGLE_SPREADSHEET_SHARE_LINK" variable for the next step.   

### Save data for all episodes in its season's manifest file:
Save the "GOOGLE_SPREADSHEET_SHARE_LINK" and other properties, like "GOOGLE_SPREADSHEET_URL" in an "episode" section in that season's manifest file. Once all episodes for a given season have been updated, the season's manifest file must be manually uploaded to it location in S3. At bucket `media.angel-nft.com` and directory structure `tuttle_twins/manifests/<season_code>-episodes.json`.

## Scheduled dataset updates
A cron schedule drives execution of the `data_preparation.py` script. This script processes each episode of each season manifest file found in the manifests directory in s3.  

The google spreadsheet for a given episode is loaded into a dataframe. Once each image frame is randomly assigned to one of the three datasets the dataframe defines the mapping for each source jpg file to its new destination dataset folder.

Recursive S3 searches are used to create a dataframe that describes all source image files for the given episode and a another dataframe that describes the location of all episode jpg files found in the target dataset directories.

Dataset operations are used bring the destination datasets in sync with the google spreadsheet for that episode.  Dataframe operations are used to find 4 change sets:

1. All destination jpg files that are already present at its new destination dataset and do not need to be altered.  
2. All destination jpg files that must be deleted, since it is no longer mapped from a source file.  
3. All destination jpg files that need to be moved from one destionation dataset to another.  
4. All source jopg files that need to be copied to a given destination dataset.  

AWS Boto3 functions are used to copy and delete lists ofS3 files as needed.
### Logging
Each scheduled run of `tuttle-twins-data-prep.py` will be logged and tracked using AWS Cloud Watch.

If a given run fails, Cloud Watch will trigger an event that causes an SMS error message be sent to operations.

Tuttle Twins dataset updates are not directly consumer-facing, so immediate action need not be taken.

## AWS CLI installtion:
```
sudo yum update -y
sudo yum install unzip
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
```

## Set up the local virtual environment:
Use these commands to setup the local virtual environment and to install all python packages required for this project:
```
python3 -m venv venv
source venv/bin/python (to activate the virtual environment)
python3 -m pip install --upgrade pip
pip install -r requirements.txt (to install all packages into your virtual environment)
```

Use this to deactivate your virtual environment:   
```
deactivate 
```

## VSCode settings:
In vscode open the Command Controller using shift-cmd-p  
Choose "Select Python Interpreter"  
Enter  `./venv/bin/python`  
## Environment settings:
The local `.env` file describes settings that are loaded by the application and used for local development.

