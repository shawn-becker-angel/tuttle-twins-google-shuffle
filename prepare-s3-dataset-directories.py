
# choose a dedicated server
# copy manifest.jl files to server
#
# specify s3_src_bucket
# specify s3_dst_bucket
# for each manifest_file:
#   read manifest_file into a dataframe
#   find classes
#   for each row in manifest
#     create src_key as src_bucket/image.jpg
#     create dst_key as dst_bucket/class
#     s3.copy(src_bucket, src_key, dst_bucket)

import json
import boto3

src_bucket_name = 'dst_bucket'
dst_bucket_name = 'dst_bucket'

def copy_s3_files(manifest_rows, src_bucket_name, dst_bucket_name):
    '''
    adapted from https://stackoverflow.com/a/47468350/18218031
    '''
    s3 = boto3.resource('s3')
    dst_bucket = s3.Bucket(dst_bucket_name)
    copy_source = {
      'Bucket': src_bucket,
      'Key': None
    }
    for row in manifest_rows:
        src_key = src_channel + '/' + row['img-ref']
        dst_key = dst_channel + '/' + row['class']
        copy_source['Key'] = src_key
        # dst_bucket.copy(copy_source, dst_key)
        print("dst_bucket.copy(copy_source:", str(copy_source), "dst_key:", dst_key)

manifest_files = [
    'S01E01-manifest.jl',
    'S01E02-manifest.jl'
]

for manifest_file in manifest_files:
    with open(manifest_file, "r") as mf:
        manifest_lines = mf.readlines()
        manifest_rows = []
        for json_line in manifest_lines:
            manifest_rows.append(json.loads(json_line))
        copy_s3_files(manifest_rows, src_bucket_name, dst_bucket_name)
