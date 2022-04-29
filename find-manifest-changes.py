import typing
from typing import Any
import re
import boto3

 
 # Every TuttleTwins episode manifest_file has 
 # format:
 #    S<season>E<episode>-manifest-<utc-datetime-iso>.jl
 # for example:
 #    S01E01-manifest-2022-04-28T10:43:48.733843.jl
 # Read the contents of the latest version of the given manifest file (all json lines)
 # or return None if not found.
 #
 # define the search filter as "S{season}E{episode}-manifest-"

def get_latest_manifest_in_s3(manifest_bucket: str, manifest_file: str=None, season: str=None, episode: str=None) -> Any:
   if manifest_file is not None:
      manifest_file_pattern = "^S(\d\d)E(\d\d)-manifest-(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)\.(\d{6})\.jl$"
      z = re.match(manifest_file_pattern, manifest_file)
      (season, episode, yyyy, mm, dd, hh, min, sec, millis) = z.groups()
   elif season is None or episode is None:
      raise Exception("season and episode are required when manifest_file is None")

   s3 = boto3.resource('s3')
   s3client = boto3.client('s3')

   bucket = s3.Bucket(manifest_bucket)
   objects = bucket.
   s3_find_prefix = f"S{season}E{episode}-manifest-"
   s3_object_summary_list = bucket.objects.filter(Prefix=find_prefix)
   latest_s3_object = None
   for s3_object_summary in s3_object_summary_list:
      metadata = s3client.head_object(Bucket='MyBucketName', Key=file['Key'])


    dst_bucket = s3.Bucket(dst_bucket_name)
