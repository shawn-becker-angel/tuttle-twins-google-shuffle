import typing
from typing import Any, List
import re
import os
import boto3
import manifest_row
from env import S3_MEDIA_ANGEL_NFT_BUCKET, S3_MANIFESTS_DIR

from s3_utils import s3_download_text_file

def s3_find_latest_manifest(manifest_jl_file: str) -> List[ManifestRow]:
   '''
   Every TuttleTwins episode manifest_jl_file has 
   format:
      S<season>E<episode>-manifest-<utc-datetime-iso>.jl
   for example:
      S01E01-manifest-2022-04-28T10:43:48.733843.jl
   
   This function creates a search pattern from the given locally generated 
   <manifest_jl_file> stored under LOCAL_MANIFESTS_DIR
   This search pattern is used to find all matching manifests in s3 that differ only in the version datetime <utc-datetime-iso>
   If any matching manifest files are found in s3, this function return the jl file contents of the most recent version.

   The jl file contents is a list of dict where each dict contains two string properties
   '''
   if manifest_jl_file is not None:
      manifest_jl_file_pattern = "^S(\d\d)E(\d\d)-manifest-(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)\.(\d{6})\.jl$"
      z = re.match(manifest_jl_file_pattern, manifest_jl_file)
      (season, episode, yyyy, mm, dd, hh, min, sec, millis) = z.groups()
   elif season is None or episode is None:
      raise Exception("season and episode are required when manifest_jl_file is None")

   s3 = boto3.resource('s3')
   s3client = boto3.client('s3')

   bucket = s3.Bucket(S3_MEDIA_ANGEL_NFT_BUCKET)
   prefix = f"{S3_MANIFESTS_DIR}/S{season}E{episode}-manifest-"
   matches = []
   s3_object_summary_list = bucket.objects.filter(Prefix=prefix)
   if list(s3_object_summary_list.limit(1)):    # if at least 1 s3_object was fouind
      for s3_object_summary in s3_object_summary_list:
         key = s3_object_summary.key
         metadata = s3client.head_object(Bucket=S3_MEDIA_ANGEL_NFT_BUCKET, Key=key)
         lastModified = metadata['LastModified']
         matches.append( {"key": key, "lastModified":lastModified.isoformat()} )

   if len(matches) == 0:
      return None

   ordered = sorted(matches, key = lambda i: i['lastModified'], reverse=True)
   return ordered[0]

def download_s3_file(bucket: str, prefix: str, local_file: str) -> int:
   '''
   download a file from s3, store it to local_file, and return the number of lines in the file
   otherwise return 0
   '''



if __name__ == "__main__":
   test_local_manifest_jl_file = "S01E01-manifest-2022-05-02T12:43:24.662714.jl"

   s3_latest = s3_find_latest_manifest(manifest_jl_file=test_local_manifest_jl_file)

   local_jl_file = "/tmp/latest.jl"

   s3_download_text_file(bucket=S3_MEDIA_ANGEL_NFT_BUCKET, key=s3_latest['key'], dn_path=local_jl_file)

   # print the first 10 lines of  local_jl_file
   with open(local_jl_file, "r") as f:
      for i in range(10):
         print(f.readline())
