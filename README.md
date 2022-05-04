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
Tuttle Twins has 1 or more "seasons" and each "season" may have as many as 12 "episodes"
### 1. The NFT team manages a "Google Episode Spreadsheet" for each episode in each season.  
    a. Each row in an episode spreadsheet has the following columns:
      * `S3_BASE_URL`: example: 
        `https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/`
      * `ROW_INDEX`: starts with 0
      * `FRAME NUMBER`: example: `TT_S01_E01_FRM-00-00-08-12`
      * `UNSUPERVISED CLASSIFICATION`: see `RARITY_CHOICES`
      * `SUPERVISED CLASSIFICATION`: see `RARITY_CHOICES`
      * `JONNY's RECLASSIFICATION`: see `RARITY_CHOICES`
      * `METADATA`: example `[Emily, Space Tunnel, Time Machine]`  
    b. `RARITY_CHOICES` can be one of `(Junk, Common, Uncommon, Rare, or Legendary)`
    c. S3_BASE_URL and the FRAME NUMBER are used to compute the S3 src_url. example:
        `https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/TT_S01_E01_FRM-00-00-08-12.jpg` 
    d `ROW_INDEX` and `METADATA` are not used for this project

### 2. "Season Manifest Files" (a json file):  
    a. A Season Manifest File defines the Episode Google Sheet used for each episode in that season.
    b. This is a json text file that is manually created and uploaded to S3 whenever a new episode is published  
    c. Periodic re-generated of Episode Manifest Files is directed by its Season Manifest File.  
    d. Example:  
      ```
      aws s3 ls s3://media.angel-nft.com/tuttle_twins/manifests/ | grep "-episodes.json"  
      2022-05-04 13:36:39        962 S01-episodes.json  
      ```
    e. This json file consists of a list of "Episode Objects" (python dict), each with these attributes:  
      * `season_code` a string, episode's season code, example `S01`  
      * `episode_code`: a string, episode's episode number, example `E07`  
      * `spreadsheet_title`: string title of the epiode's Google sheet  
      * `spreadsheet_url`: URL, of the google spreadsheet, which can be used for editing by designated users only
      * `share_link` a manually created URL, that anyone can use to view the google spreadsheet  
      * TODO remove local_manifest_jl_file attribute, since each episode has many versioned episode manifest files in S3

### 3. "Versioned Episode Manifest Files" (a file of json lines):  

    a. Many Versioned Episode Manifest files are stored in S3 for a given episode.
    b. A new Versioned Episode Manifest file is periodically re-generated from the data available in its associated Episode Google Sheet.
    c. A newly generated Episode Manifest file is only uploaded to S3 if it differs from the previous latest version in S3
    d. Example:
      ```
      (venv) ~/workspace/tuttle-twins-rarity$ aws s3 ls s3://media.angel-nft.com/tuttle_twins/manifests/ | egrep "S\d\dE\d\d-.*\.jl"  
      2022-05-02 14:54:19    4744253 S01E01-manifest-2022-05-02T12:43:24.662714.jl  
      2022-05-02 14:54:19    4744253 S01E01-manifest-2022-05-02T13:09:44.722111.jl  
      ```
### 4, "Episode Manifest Row" (a python dict):  
    a. Each row of the Episode Manifest file describes:
      * 'src_url`, the S3 URL of the image jpg file of a given frame
      * `dst_key`, the S3 KEY of the copied image file
    b. Machine Learning (ML) algorithms require that images be randomly shuffled into different folders, one for each processing stage:
      * a random selection of 70% of all pre-classified images are copied into the "train" folder
      * a random selection of 20% of all pre-classified images are copied into the "validate" folder
      * a random selection of 10% of all pre-classified images are copied into the "test" folder
    c. Example:
    ```
      {  "src_url": "https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/stamps/TT_S01_E01_FRM-00-00-09-00.jpg",   
         "dst_key": "tuttle_twins/s01e01/ML/train/Rare/TT_S01_E01_FRM-00-00-09-00.jpg"   } 
    ``` 
### 5. `create-manifests.py` (a python module):  
    a. This python module creates an Episode Manifest File for each Episode Object defined in a Season Manifest File 
    b. each episode spreadsheet is downloaded from Google Docs using google's API functions installed via its `spread` package 
    c. google sheet processing API functions require a GOOGLE_CREDENTIALS_FILE   
    
### 6. `preview-manifest.ipynb` (a jupyter notebook):    
    a. this Jupyter notebook is used to view a sampled selection of the image frames defined in a given `episode manifest file` downloaded from S3
    b. it uses the local `plot_hist_lib/plot_image_histogram.py` module to render a given RGB image  
    c. a "standardized" version of the image is then computed where:  
      * each 8-bit RGB pixel is converted to a floating-point GRAYscale value
      * pixel values are shifted so that the mean of all pixel values in the image is 0.0  
      * pixel values are scaled so that the stddev of all pixel values in the frame is 1.0  
    d. a histogram of the standardized image is then computed and superimposed on the RGB image  

    NOTE: justifications for image data "normalization" by mean and stddev:  
    * `https://becominghuman.ai/image-data-pre-processing-for-neural-networks-498289068258`   
    * `https://stats.stackexchange.com/a/220970`   
    * `https://towardsdatascience.com/normalization-vs-standardization-which-one-is-better-f29e043a57eb`  

### 7. `process_manifests.py` (a python module) 
This module copies  notebook file arranges image files into the class-sensitive directory structure required by Tensorflow image classification libraries:  

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

## Tuttle Twins Seasons and Episodes:  
There may be many seasons and each season has several episodes

## Episode Google Sheet:  
An Episode Google Sheet is a spreadsheet that contains data for thousands of frames in a single TuttleTwins Episode
Each row describes the S3 Image URL, Frame Number, Classifications, and Tags for a given frame
This spreadsheet can be manually altered by the NFT team at any time

## S3 Image Url:  
Pre-sized versions of each image frame are stored as JPG files in s3:  
* high_res: 1920x1080 pixels
* mid_res: 1024x576 pixels
* low_res: 854x480 pixels
* thumbnails: 640x360 pixels

Example URL:  
`https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/TT_S01_E01_FRM-00-00-08-21.jpg`  

Example S3 object query:  
`aws s3 ls s3://media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/TT_S01_E01_FRM-00-00-08-21.jpg`  
## Classifications:  
### Ranks:
The Episode Google Sheeth has Classification columns whose values may be one of 5 rankings:
  Junk, Common, Uncommon, Rare, and Legendary  

### Three Classification Columns:  
* Unsupervised classifictions (Common, Uncommon, Rare) are done using K-Means clustering of image metrics (what image metrics?)  
* Supervised classifications (Junk, Common, Uncommon, Rare) are entered manually (using what tagging tool?)  
* Jonny's classifications (Legendary) are entered manually (using what tagging tool?)  




## S3 Key Row:  
This class describes attributes of each  row output from an 'aws s3 ls search'
```
Example row of outpout:
"2022-05-03 19:15:34       2715 tuttle_twins/s01e01/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-13-19.jpg"  

Parsed S3 Key Row attributes:
{ "last_modified": "2022-05-03T19:15:34", "size": 2715, "key":"tuttle_twins/s01e01/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-13-19.jpg"}
```

