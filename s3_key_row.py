from typing import TypedDict
import datetime


# describes aws s3 ls results as a dict with typed attributes
class S3KeyRow:
    last_modified: datetime.datetime
    size: int
    key: str
    
    def __init__(self, s3_ls_line: str=None, s3_key_dict: dict=None):
        if s3_ls_line is not None:
            '''
            parse one line of the result of 'aws s3 ls --recursive <path>', e.g.
            "2022-05-03 19:15:44       2336 tuttle_twins/s01e01/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"
            parsed into dict, e.g.
            {"last_modified": 2022-05-03T19:15:44 , "size":2336, "key":"tuttle_twins/s01e01/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"}
            '''
            parts = s3_ls_line.split(" ")
            self.last_modified = datetime.datetime((parts[0] + "T" + parts[1]), '%Y-%m-%dT%H:%M:%S') 
            self.size = int(parts[2])
            self.key = parts[3]
        elif s3_key_dict is not None:
            self.last_modfied = s3_key_dict['last_modified']
            self.size = s3_key_dict['size']
            self.key = s3_key_dict['key']
        else:
            raise Exception("S3KeyRow requires either s3_ls_line or s3_key_dict argument")

    def as_dict(self):
        return { "last_modified" : self.last_modified, "size": size, "key": key }
    