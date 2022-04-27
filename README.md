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



