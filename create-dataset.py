
# choose a dedicated server
# copy episode manifest.jl files to server
#
# specify s3_src_bucket
# specify s3_dst_bucket
# for each manifest_jl_file:
#   read manifest_jl_file into df
#   create columns "src-key" and "dst-key"
#   for each row in df
#     s3.copy(src_bucket, src_key, dst_bucket, dst_key)

import json
import boto3

src_bucket_name = 'src_bucket'
dst_bucket_name = 'dst_bucket'

# def copy_s3_files(df, src_bucket_name, dst_bucket_name):
#     '''
#     adapted from https://stackoverflow.com/a/47468350/18218031
#     '''
#     s3 = boto3.resource('s3')
#     dst_bucket = s3.Bucket(dst_bucket_name)
#     copy_source = {
#       'Bucket': src_bucket,
#       'Key': None
#     }
#     for row in manifest_rows:
#         src_key = src_channel + '/' + row['img-ref']
#         dst_key = dst_channel + '/' + row['class']
#         copy_source['Key'] = src_key
#         # dst_bucket.copy(copy_source, dst_key)
#         print("dst_bucket.copy(copy_source:", str(copy_source), "dst_key:", dst_key)

# def get_season_episodes_file():
#     return "episodes/tuttle-twins-S01-episodes.json:
#     return 
# def find_latest_episode_manifest_jl_files():
#     return [
#     'S01E01-manifest.jl',
#     'S01E02-manifest.jl'
# ]

# # {"src-ref":"https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/s01e01/default_eng/v1/frames/stamps/TT_S01_E01_FRM-00-00-08-09.jpg","class":"Common"}

# episode_manifest_jl_files = find_latest_episode_manifest_jl_files()

# for episode_manifest_jl_file in episode_manifest_jl_files:
#     season_episode_key = 
#     df = pd.read_json(manifest_jl_file)
#     src_base = "https://s3.us-west-2.amazonaws.com/media.angel-nft.com/tuttle_twins/" + season_episode_key + s01e01/default_eng/v1/frames/stamps/
#     df['file'] = df['src-key'].str.replace(base_url, "")
#     with open(, "r") as mf:
#         df = pd.read_json()
#         manifest_lines = mf.readlines()
#         manifest_rows = []
#         for json_line in manifest_lines:
#             manifest_rows.append(json.loads(json_line))
#         copy_s3_files(manifest_rows, src_bucket_name, dst_bucket_name)
