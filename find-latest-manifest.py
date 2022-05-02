import typing
from typing import Any
import re
import boto3

 
 # Every TuttleTwins episode manifest_file has 
 # format:
 #    S<season>E<episode>-manifest-<utc-datetime-iso>.jl
 # for example:
 #    S01E01-manifest-2022-04-28T10:43:48.733843.jl
 #
 # Create a search pattern from the given manifest_file
 # Use the search pattern to find all matching manifests in s3
 # Find the latest and return its jl file contents
 

def s3_find_latest_manifest(manifest_bucket: str, manifest_dir: str, manifest_file: str) -> Any:
   if manifest_file is not None:
      manifest_file_pattern = "^S(\d\d)E(\d\d)-manifest-(\d\d\d\d)-(\d\d)-(\d\d)T(\d\d):(\d\d):(\d\d)\.(\d{6})\.jl$"
      z = re.match(manifest_file_pattern, manifest_file)
      (season, episode, yyyy, mm, dd, hh, min, sec, millis) = z.groups()
   elif season is None or episode is None:
      raise Exception("season and episode are required when manifest_file is None")

   s3 = boto3.resource('s3')
   s3client = boto3.client('s3')

   bucket = s3.Bucket(manifest_bucket)
   prefix = f"{manifest_dir}/S{season}E{episode}-manifest-"
   matches = []
   s3_object_summary_list = bucket.objects.filter(Prefix=prefix)
   if list(s3_object_summary_list.limit(1)):    # if at least 1 s3_object was fouind
      for s3_object_summary in s3_object_summary_list:
         key = s3_object_summary.key
         metadata = s3client.head_object(Bucket=manifest_bucket, Key=key)
         lastModified = metadata['LastModified']
         matches.append( {"key": key, "lastModified":lastModified.isoformat()} )

   if len(matches) == 0:
      return None

   ordered = sorted(matches, key = lambda i: i['lastModified'], reverse=True)
   return ordered[0]


if __name__ == "__main__":
   manifest_bucket = "media.angel-nft.com"
   manifest_dir="tuttle_twins/manifest"
   manifest_file = "S01E01-manifest-2022-04-28T10:43:48.733843.jl"

   latest = s3_find_latest_manifest(manifest_bucket=manifest_bucket, manifest_dir=manifest_dir, manifest_file=manifest_file)
   print(latest)
