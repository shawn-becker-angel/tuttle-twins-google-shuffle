import datetime
import re

# this class describes the attributes of each key of an 'aws s3 ls' search result in the tuttle_twins ML folder
class s3_episode_key:  
    
    attrs = {}
            
    # e.g. key -> tuttle_twins/ML/validate/Rare/TT_S01_E01_FRM-00-00-09-01.jpg
    key_regex_patterns = {
        "folder": r"^\w+/ML/(\w+)/\w+/TT_S\d\d_E\d\d_FRM-.+\.jpg$",
        "img_class": r"^\w+/ML/\w+/(\w+)/TT_S\d\d_E\d\d_FRM-.+\.jpg$",
        "img_frame" : r"^\w+/ML/\w+/\w+/(TT_S\d\d_E\d\d)_FRM-.+\.jpg$",
        "season_code": r"^\w+/ML/\w+/\w+/TT_(S\d\d)_E\d\d_FRM-.+\.jpg$",
        "episode_code": r"^\w+/ML/\w+/\w+/TT_S\d\d_(E\d\d)_FRM-.+\.jpg$"
    }
                  
    def __init__(self, s3_ls_line: str=None, other_attrs: dict=None):
        if s3_ls_line is not None:
            
            # Take one line of the result of 'aws s3 ls --recursive <path>', e.g.
            # "2022-05-03 19:15:44       2336 tuttle_twins/s01e01/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"
            # and split it into the 3 attributes of the standard s3 object:
            # {"last_modified": 2022-05-03T19:15:44 , "size":2336, 
            # "key":"tuttle_twins/s01e01/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"}

            line = ' '.join(s3_ls_line.split())
            parts = line.split(" ")
            self.attrs['last_modified'] = datetime.datetime.strptime(parts[0] + "T" + parts[1], '%Y-%m-%dT%H:%M:%S') 
            self.attrs['size'] = int(parts[2])
            key = self.attrs['key'] = parts[3]

            # then use the key_regex_patterns to parse the key into these 5 key attributes:
            # {"folder":"validate", "img_class":"Uncommon", 
            # "img_frame":"TT_S01_E01_FRM-00-19-16-19", 
            # "season_code": "S01", "episode_code": "E01"}
            
            for i, (k, ptn) in enumerate(self.key_regex_patterns.items()):
                result = re.search(ptn, key)
                self.attrs[k] = result.group(1)

            # and finally construct this key attribute
            self.attrs['episode_id'] = self.attrs["season_code"] + self.attrs["episode_code"]
              
        elif other_attrs is not None:
            '''
            use other_attrs to set own attrs
            '''
            self.attrs = other_attrs.copy()

        else:
            raise Exception("constructor requires either s3s_line:str or other_attrs:dict) argument")
    
    @staticmethod
    def get_columns():
        return ['last_modified', 'size', 'key', 'folder', 'img_class', 'img_frame', 'season_code', 'episode_code', 'episode_id']

    def get_attrs(self) -> dict:
        return self.attrs
  
################################
#  Tests
################################

def assert_equals(self, name, expected, result):
    assert result == expected, f"ERROR: expected {name}: {expected} not {result}"

def test_construct_from_s3_ls_line():
    s3_ls_line = "2022-05-03 19:15:44       2336 tuttle_twins/s01e01/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"
    obj = s3_episode_key(s3_ls_line=s3_ls_line)
    assert_equals("last_modified", "2022-05-03T19:15:44", obj.attrs["last_modified"].isoformat())
    assert_equals("size", 2336, obj.attrs["size"])
    assert_equals("key", "tuttle_twins/s01e01/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg", obj.attrs["key"])
    assert_equals("folder", "validate", obj.attrs["folder"])
    assert_equals("img_class", "Uncommon", obj.attrs["img_class"])
    assert_equals("img_frame", "TT_S01_E01_FRM-00-19-16-19", obj.attrs["img_frame"])
    assert_equals("season_code", "S01", obj.attrs["season_code"])
    assert_equals("episode_code", "E01", obj.attrs["episode_code"])
    assert_equals("episode_id", "D01E01", obj.attrs["episode_id"])

  
if __name__ == '__main__':
    test_construct_from_s3_ls_line()
