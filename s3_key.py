from typing import TypedDict
import datetime

# describes each row of an 'aws s3 ls' search results as a dict with typed attributes
class s3_key:
    last_modified: datetime.datetime
    size: str
    key: str
    
    def __init__(self, s3_ls_line: str):
        '''
        Take one line of the result of 'aws s3 ls --recursive <path>', e.g.
        "2022-05-03 19:15:44       2336 tuttle_twins/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg\n"
        remove the trailing "\n
        and parse it into a s3_key dict, e.g.
        {"last_modified": 2022-05-03T19:15:44 , "size":2336, "key":"tuttle_twins/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"}
        '''
        try:
            # replace multi-spaces with one
            s3_ls_line = ' '.join(s3_ls_line.split())
            # remove trailing newline
            s3_ls_line = s3_ls_line.replace("\n", "")
            parts = s3_ls_line.split(" ")
            dt_str = parts[0] + "T" + parts[1]
            self.last_modified = datetime.datetime.strptime(parts[0] + "T" + parts[1], "%Y-%m-%dT%H:%M:%S")
            self.size = parts[2]
            self.key = parts[3]
        except Exception as exp:
            print(type(exp),str(exp))
            raise

    def get_last_modified(self):
        return self.last_modified

    def get_size(self):
        return self.size
        
    def get_key(self):
        return self.key
        
    def as_dict(self):
        '''
        Return an explicit s3_key dict
        '''
        return { "last_modified" : self.last_modified, "size": self.size, "key": self.key }
    
############################
# TESTS
############################

def test_multi_lines():
    s3_ls_lines = []
    s3_ls_lines.append("2022-05-07 02:16:43       2632 tuttle_twins/ML/test/Common/TT_S01_E01_FRM-00-00-13-01.jpg\n")
    s3_ls_lines.append("2022-05-07 02:16:43       2703 tuttle_twins/ML/test/Common/TT_S01_E01_FRM-00-00-13-04.jpg\n")
    s3_ls_lines.append("2022-05-07 02:16:43       2748 tuttle_twins/ML/test/Common/TT_S01_E01_FRM-00-00-13-22.jpg\n")
                       
    keys = []
    for s3_ls_line in s3_ls_lines:
        key = s3_key(s3_ls_line)
        keys.append(key)
    expected = 3
    result = len(keys)
    assert result == expected, f"ERROR: expected num key: {expected} not {result}"

if __name__ == '__main__':
    test_multi_lines()
    
