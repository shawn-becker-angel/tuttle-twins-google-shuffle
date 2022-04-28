# Tuttle Twins Season 1 processor

Motivation:
Image frames from the animated "Tuttle Twins" production are planned to be sold to the public as "Non-Fungible Tokens" (NFTs). 
A starting sell price for each NFT will be  determined according to some measure of its quality. One way to estimate quality 
is to base it on its level of "rarity". Categories of rarity range from the highest level of "Legendary" down to lower levels of
"Rare", "Uncommon", "Common", and "Trash".

This project uses "image-classification" algorithms with a "convolutional neural network" to automatically estimate the level of 
rarity for a given image frame. This requires first gathering a large set of images (thousands or millions of frames) that have 
been manually categorized level of "rarity". 

A sub-set of these pre-classified image frames (usually 80%) are used to train a "rarity" prediction model.

This prediction model will be used to predict the level of "rarity" for a "test" set of Tuttle Twins image frames that have
not been included in the original training set. The percentage of mis-classified image frames are used as a measure of prediction 
accuracy. Errors will be used to incrementally improve the accuracy, or "tune", the prediction model. This process continues 
until the percentage of classification errors falls below a designated threshold.

At some point, the model will be used to classify a totally new set of image frames. The results will be manually reviewed and 
those classification errors will also be used to further improve the accuracy of the model.

Related work:
Once we have gained practical experience training and improving the prediction model of image rarity, we might choose to apply 
similar processes to automatically detect scene changes, detect objects in each frame, and eventually even use facial recognition 
algorithms to describe actors that are present in each scene.

Implementation:
The project is divided into different stages of work: Data Preparation, Model Training, Model Validation, Error Evaluation, and 
Iterative Tuning. 

Each stage is implemented using pre-built Tensorflow packages, custom python modules, and data structures.

Data Preparation:
1. The NFT team creates a Google spreadsheet for each episode in season 1
    a. Each row in an episode spreadsheet has the following columns:
        S3_BASE_URL: str, (see below)
        ROW_INDEX: int, starts with 0
        FRAME NUMBER: str, example: 'TT_S01_E01_FRM-00-00-08-12'
        UNSUPERVISED CLASSIFICATION: RARITY_CHOICE
        SUPERVISED CLASSIFICATION: RARITY_CHOICE (optional)
        JONNY's RECLASSIFICATION: RARITY_CHOICE (optional)
        METADATA: subjects_list, example ['Emily', 'Space Tunnel', 'Time Machine']
    b. RARITY_CHOICE can be one of  (Junk, Common, Uncommon, Rare, Legendary) 
    c. S3_BASE_URL, example: 'https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/thumbnails/'
 
 2. create-manifests-from-google-sheets.py
    a. creates a manifest file for all episode spreadsheets in the season
    b. the list of episode spreadsheets to be processed in defined in
       "episodes/tuttle-twins-S01-episodes.json"
       this json file consists of a list of episode objects
       each episode object has these attributes:
        "season" a string, episode's season number, example "S01"
        "episode": a string, episode's episode number, example "E07"
        "spreadsheet_title": string title of the epiode's Google sheet
        "spreadsheet_url": URL, created automatically, which can only be used by designated users
        "share_link" a URL, created manually in Google Documents, which anyone can use to open the Google Sheet
        "manifest_file": a local path for the newly created manifest file
    b. google sheet processing requires credentials, which are found in "google-drive-credentials/gsheets-pyshark-348317-2b9d25a0fa1e.json"
    c. each episode spreadsheet is downloaded from Google Docs
    d. each row of an episode spreadsheet is used to create a manifest row with attributes:
        "img-ref", str, the URL to a single image frame stored in S3
        "class", str, the preferred RARITY_CHOICE among the 3 CLASSIFACTION columns described above
          where JONNY's RECLASSIFICATION > SUPERVISED CLASSIFICATION > UNSUPERVISED CLASSIFICATION
    e. all manifest rows are output as a json lines file to the episode's manifest_file (described above)

  3. preview-manifest-images.ipynb  
    a. this Jupyter notebook is used to view a sampled selection of the images defined in a given episode manifest file  
      NOTE: 
        in the current implementation the episode manifest file is assumed to have been uploaded to a given s3 location
        and is a csv file instead of a json lines fle
    b. it uses the local plot_hist_lib/plot_image_histogram.py module to render a given RGB image
    c. a "standardized" version of the image is then computed where
      each RGB pixel is converted to GRAYscale
      each grayscale pixel is converted from byte to float
      pixel values are shifted so that the mean of all pixel values in the image is 0.0
      pixel values are scaled so that the stddev of all pixel values in the frame is 1.0
    d. a histogram of the standardizd image is then computed and superimposed on the RGB image

  4. create-datasets.py
    Tensorflow image classification libraries create a "model" by processing a set of "training" images
    Training images described in each episode manifest file must be arranged in a certain format:
    a. each episode manifest file to copy each s3 image to its class folder in s3, where
      each image at source location
        s3://<src-bucket>/bucket/<src-key> where <src-key> = <src-folder>/<file>.jpg 
      is copied to its destination location 
	    s3://<dst-bucket>/<dst-key> where <dst-key> = <dst-folder>/<category>/<file>.jpg 

Model Training:
    
Model Validation:
    
Rarity Estimation:
    
Error Evalution:
    
Iterative tuning:
=======
# create-manifests

## create manifest files for tuttle twins season 01 episodes

## Use manifest files to prepare s3 directory image dataset for Tensorflow image classification 

Tensorflow image classification requires preparation of an image dataset in s3 with this structure:
s3://dst_bucket/dst_folder/class_tag/jpg_file

Millions of pre-processed src_jpg_files have already been uploaded to s3://src_bucket/src_key.
Each src_jpg_file has already been manually classified with a single enumerated class_tag.
Each jsonline in a manifest_file describes the s3://src_bucket/src_folder/src_key and class_tag for a given src_jpg_file.

In order to efficiently copy the src_jpg_files would it be possible to loop thru each class_tag and use an s3 batch copy job to copy matching src_jpg_files to their proper s3://dst_bucket/dst_folder/class_tag/jpg_file destination?

aws s3 sync s3://src_bucket/src_path/ s3://dst_bucket/dst_path/class --filter tag=class

Or would it be simpler to just use `aws s3 cp` to individually copy each src_jpg_file to its target s3 destination?


