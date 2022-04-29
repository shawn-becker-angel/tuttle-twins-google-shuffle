 # episode manifest files are timestamped

 def find_episode_manifest_file_pairs():
     '''
     create a list of last and prev manifest pairs for each season-episode. 
     Note that newly-added episodes will have only a last manifest without a prev.
     [
        {"season":"S01", "episode":"E01", 
           "last": "manifest-2021-04-28T12:00:123456-00:00.jp",
           "prev": "manifest-2021-04-28T12:00:345612-00:00.jp"
        },
        {"season":"S08", "episode":"E8", 
           "last": "manifest-2022-04-28T12:00:123456-00:00.jp",
           "prev": null
        },           
     ] 
     '''