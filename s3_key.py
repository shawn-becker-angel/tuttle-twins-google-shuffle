from typing import TypedDict
import datetime

# describes each row of an 'aws s3 ls' search results as a dict with typed attributes
class s3_key(TypedDict):
    last_modified: datetime.datetime
    size: str
    key: str
    
    def __init__(self, s3_ls_line: str=None, s3_key_dict: dict=None):
        if s3_ls_line is not None:
            '''
            Take one line of the result of 'aws s3 ls --recursive <path>', e.g.
            "2022-05-03 19:15:44       2336 tuttle_twins/s01e01/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"
            and parse it into a s3_key dict, e.g.
            {"last_modified": 2022-05-03T19:15:44 , "size":2336, "key":"tuttle_twins/s01e01/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"}
            '''
            parts = s3_ls_line.split(" ")
            self.last_modified = datetime.datetime((parts[0] + "T" + parts[1]), '%Y-%m-%dT%H:%M:%S') 
            self.size = int(parts[2])
            self.key = parts[3]
        elif s3_key_dict is not None:
            '''
            Take the attributes of another s3_key dict to set attributs
            '''
            self.last_modified = s3_key_dict['last_modified']
            self.size = s3_key_dict['size']
            self.key = s3_key_dict['key']
        else:
            raise Exception("s3_key requires either s3_ls_line or s3_key_dict argument")

    def as_dict(self):
        '''
        Return an explicit s3_key dict (perhaps not needed)
        '''
        return { "last_modified" : self.last_modified, "size": self.size, "key": self.key }
    