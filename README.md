# The Tuttle Twins Rarity Estimation Project   
## Motivation:  
The Angel Studios website (https://www.angel.com) and apps are introducing new functionality that allows users to purchase a "Non-Fungible Token" (NFT) for any image frame from the animated "Tuttle Twins" production. A starting sell price for each NFT will be  determined according to some measure of its quality. The initial measure of a frame's quality is being based on its level of "rarity". Categories of rarity range from the highest level of "Legendary" down to lower levels of "Rare", "Uncommon", "Common", and "Trash".  

This project uses "image-classification" algorithms with "convolutional neural networks" (CNNs) to automatically estimate the level of rarity for a given image frame. This requires first creating a large set (thousands or millions) of frames that have been manually assigned a "rarity" level. Image frames are extracted from each episode's digital MP4 video file. Multiple rarity assignments are collected from both automated and manual sources. Ultimately, only the most trusted assignment is used for each image frame.

A large sub-set of these pre-classified image frames (e.g. 80%) is used to "train" a prediction model.  

Once the prediction model is trained, the remaining set of classified image frames (e.g. 20%) that have not been included in the training set, is used to "validate" the prediction model. The percentage of miss-classified validation image frames are used as a measure of prediction accuracy. The percentage of errors is used to incrementally improve the accuracy of the model. This process continues until the percentage of classification errors falls below some designated threshold. This iterative process may also stop if the rate of prediction accuracy stops improving.  

At some point, the model will be used to classify a totally new set of image frames. The results will be manually reviewed and those classification errors will be used to further improve the accuracy of the model.

## Related work:
Once we have gained practical experience training and improving the prediction model using CNNs, we may choose to apply similar processes to do any of the following:  
* automatically detect scene changes and group frames into scenes  
* automatically detect known shapes (e.g. cars, faces, or rocket ships) in each frame  
* use facial recognition algorithms to detect actors that are present in each scene  

## Implementation:  
Implemenation of the Tuttle Twins Rarity Estimation project is divided into 5 different stages of work:  
* Data Preparation  
* Model Training  
* Model Validation  
* Error Evaluation  
* Iterative Tuning  

Each stage is implemented using pre-built Tensorflow packages, custom python modules, and data structures running on high-powered servers at Amazon Web Serices or Google Cloud.

## Google Drive Setup  

### Create a new Google Cloud project  
Go to `https://console.developers.google.com/`  
Click "create a new project" link named "gsheets-pyshark"  
Click on top right bell icon to see a notification that the project has been created  
Click "View" to view the detail page of the newly created project  
Save your `project_service_account email`   

### Create Keys for your new Google Cloud Project  
Click the "Keys" tab, click "Add Key", select "Create New Key", select Key type "JSON", click "CREATE" link  
Copy the new JSON credentials file from your Downloads folder to a safe location  
Set credentials_file variable to this location  

## Setup a Google Drive spreadsheet  
### Open an existing Google Drive spreadsheet  
Go to url `https://docs.google.com/spreadsheets/d/1cr_rXVh0eZZ4aLtFKb5dw8jfBtksRezhY1X5ys6FckI/edit#gid=1690818184`  
Ensure that the first row is column names  
### Or create a new Google Drive spreadsheet  
Go to `https://docs.google.com/spreadsheets/u/0/?tgif=d` and click "Blank" to create a new spreadsheet  
Rename the newly created sheet to "pyshark tutorial (`https://pyshark.com/google-sheets-api-using-python`)"  
Fill out the new spreadsheet - first row is column names  
## Share the Google Drive spreadsheet with `project_service_account_email`  
Click "Share" button at top right  
Under "Share with people and groups" add your `project_service_account_email` from above  
Alert window says "You are sharing to `project_service_account_email` who is not in the Google Workspace organization that this item belongs to."  
Click "Share anyway" link  
Click "Copy link" and set share_link variable to this url  

## Data Preparation:  
### 1. The NFT team creates a Google spreadsheet for each episode in each season.  

    a. Each row in an episode spreadsheet has the following columns:
        S3_BASE_URL: str, (see below)
        ROW_INDEX: int, starts with 0
        FRAME NUMBER: str, example: 'TT_S01_E01_FRM-00-00-08-12'
        UNSUPERVISED CLASSIFICATION: RARITY_CHOICE
        SUPERVISED CLASSIFICATION: RARITY_CHOICE (optional)
        JONNY's RECLASSIFICATION: RARITY_CHOICE (optional)
        METADATA: subjects_list, example ['Emily', 'Space Tunnel', 'Time Machine']  

    b. RARITY_CHOICE can be one of  (Junk, Common, Uncommon, Rare, Legendary)  

    c. S3_BASE_URL, for example: 'https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/'
 
### 2. `create-manifests.py`  

    a. This python module creates a manifest file for each episode spreadsheet for each season  

    b. The list of episode spreadsheets to be processed is defined in a manually created season-episodes file, e.g. `S01-episodes.json` stored under LOCAL_SEASON_EPISODES_DIR. This json file consists of a list of episode objects  

    Each episode object has these attributes:  
    
    * `season_code` a string, episode's season code, example `S01`  
    * `episode_code`: a string, episode's episode number, example `E07`  
    * `spreadsheet_title`: string title of the epiode's Google sheet  
    * `spreadsheet_url`: URL, created automatically, which can only be   used by designated users
    * `share_link` a URL, created manually in Google Documents, which anyone can use to open the Google Sheet  
    * `manifest_jl_file`: a local path for the newly created manifest file stored under LOCAL_MANIFESTS_DIR 
    
    b. google sheet processing requires a GOOGLE_CREDENTIALS_FILE   

    c. each episode spreadsheet is downloaded from Google Docs using the spread package 

    d. each row of an episode spreadsheet is used to create a manifest row with attributes:  
    * 'img-ref`, str, the URL to a single image frame stored in S3  
    * `class`, str, the preferred RARITY_CHOICE among the 3 CLASSIFACTION columns described above  
    
    e. all manifest rows are output as a json lines file to the episode's manifest_jl_file stored under LOCAL_MANIFESTS_DIR (as described above)  

### 3. `preview-manifest.ipynb`    

    a. this Jupyter notebook is used to view a sampled selection of the image frames defined in a given `episode manifest file` saved under LOCAL_MANIFESTS_DIR.   

    NOTE: in the current implementation, the `episode manifest file` is a CSV file that has been uploaded to a given s3 location.  

    b. it uses the local `plot_hist_lib/plot_image_histogram.py` module to render a given RGB image  

    c. a "standardized" version of the image is then computed where:  
    * each RGB pixel is converted to GRAYscale  
    * each grayscale pixel is converted from byte to float
    * pixel values are shifted so that the mean of all pixel values in the image is 0.0  
    * pixel values are scaled so that the stddev of all pixel values in the frame is 1.0  

    d. a histogram of the standardized image is then computed and superimposed on the RGB image  

    NOTE: justifications for image data "normalization" by mean and stddev:  
    * `https://becominghuman.ai/image-data-pre-processing-for-neural-networks-498289068258`   
    * `https://stats.stackexchange.com/a/220970`   
    * `https://towardsdatascience.com/normalization-vs-standardization-which-one-is-better-f29e043a57eb`  

### 4. `create-datasets.py`  
This jupyter notebook file arranges image files into the class-sensitive directory structure required by Tensorflow image classification libraries:  

```
s3://dst-bucket/
  dst-folder/
    train/
      class1/
        s1-e1-c1-a.jpg
        s2-e9-c1-w.jpg
      class2/
        s9-e1-c2-m.jpg
        s8-q7-c2-p.jpg
    test/
      class1/
        s1-e7-c1-x.jpg
        s2-e9-c1-b.jpg
      class2/
        s3-e1-c2-a.jpg
        s3-q1-c2-b.jpg
```
Each `episode manifest file` is used to calculate the non-class-sensitive source path and class-sensitive destination paths for image files in the cumulative dataset.  


## Model Training:
    
## Model Validation:
    
## Error Evalution:
    
## Iterative tuning:
    
    
## Image Frame Reclassification Notes
Manual classification of all image frames is stored in Google Drive spreadsheets for each episode. New image frames can be added and old frames may be re-classified at any time.  
    
An automated process is needed to detect and handle these changes, that does not require engineering effort.  
    
At scheduled times, this process will fetch each episode manifest file and compare it with its previous timestamped version.  

For each new row found, copy the new source image file to its destination.  
    
For each row with an altered classification, copy the source image file to its new destination and then delete it from its old location.  

## Dataset Directory Structure Note:  
Tensorflow image classification requires preparation of an image dataset in s3 with the structure described above.

One way to copy millions of image files from one location to another in s3 is to copy each image individually.

AWS recommends using `aws s3 cp --recursive` for large-scale batch operations on Amazon S3 objects  
* https://docs.aws.amazon.com/AmazonS3/latest/userguide/batch-ops.html  

However, this does not provide the ability to perform a specified operation per object, so there is no way to customize the class-specific destination prefix for each object.  

Another approach (needs experimentation) is to use `aws s3 cp --recursive` on category-specific manifest files. It remains to be seen if the following command would work:  
```
  aws s3 cp --recursive \
    s3://<src-bucket>/<src-folder>/ \
    s3://<dst-bucket>/<dst-folder>/train/<class>  
```






