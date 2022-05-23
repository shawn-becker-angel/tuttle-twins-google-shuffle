import datetime

import logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger("s3_key")


# this class takes 1 s3_ls_line (1 row of an 'aws s3 ls' search result)
# and sets internal last_modified, size and key properties
class S3Key:
    last_modified: datetime.datetime
    size: str
    key: str
    
    def __init__(self, s3_ls_line: str=None, s3_line_dict: dict=None):
        '''
        Take one line of the result of 'aws s3 ls --recursive <path>', e.g.
        "2022-05-03 19:15:44       2336 tuttle_twins/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg\n"
        remove the trailing "\n
        and parse it into instance attributes.
        OR
        Take a dict to set all properties
        '''
        try:
            if s3_ls_line is not None:
                # replace multi-spaces with one
                s3_ls_line = ' '.join(s3_ls_line.split())
                # remove trailing newline
                s3_ls_line = s3_ls_line.replace("\n", "")
                parts = s3_ls_line.split(" ")
                assert len(parts) == 4, f"ERROR: expected s3_ls_line to have 4 parts not {len(parts)}"
                dt_str = parts[0] + "T" + parts[1]
                self.last_modified = datetime.datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S")
                self.size = parts[2]
                self.key = parts[3]

            elif s3_line_dict is not None:
                self.last_modified = s3_line_dict['last_modified']
                self.size = str(s3_line_dict['size'])
                self.key = s3_line_dict['key']

            self.validate_fields()
        
        except Exception as exp:
            logger.error(type(exp),str(exp))
            raise
    
    def validate_fields(self):
        assert self.last_modified is not None
        assert isinstance(self.last_modified, datetime.datetime)

        assert self.size is not None and len(self.size) > 0
        assert isinstance(self.size, str)

        assert self.key is not None and len(self.key) > 0
        assert isinstance(self.key, str)

    def get_last_modified(self) -> datetime.datetime:
        return self.last_modified

    def get_size(self) -> str:
        return self.size
        
    def get_key(self) -> str:
        return self.key
        
    def as_dict(self) -> dict:
        '''Return a new D3Key as a dict'''
        return { "last_modified" : self.last_modified, "size": self.size, "key": self.key }

@staticmethod
def get_S3Key_dict_list(s3_keys_list):
    return [key.as_dict() for key in  s3_keys_list]


if __name__ == '__main__':
    logger.info("done")

    
