import re
from time import perf_counter
import pandas as pd

# this class describes the attributes of each key of an 'aws s3 ls' search result in the tuttle_twins ML folder
class s3_episode_key:  
    
    attrs = {}
    
    # constructor        
    def __init__(self, s3_ls_line: str=None, other_attrs: dict=None, df: pd.DataFrame=None):
        if s3_ls_line is not None:
            
            attrs = {}
            attrs['s3_ls_line'] = s3_ls_line

            # s3_ls_line = "2022-05-03 19:15:44       2336 tuttle_twins/s01e01/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"
            # replace multi-spaces with single-spacs
            line = ' '.join(attrs['s3_ls_line'].split())
            
            # line = "2022-05-03 19:15:44 2336 tuttle_twins/s01e01/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"
            # split line into attrs columns ['date','time','size','key']
            attrs['date'], attrs['time'], attrs['size'], attrs['key'] = line.split(" ")

            # attrs['key'] = "tuttle_twins/s01e01/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"
            # split attrs['key'], keep attrs columns ['folder','img_class','img_frame'], ignore others with _
            (_, _, _, attrs['folder'], attrs['img_class'], attrs['img_frame'], _)  = re.split("/|\.", attrs['key'])
            
            # attrs['img_frame'] = TT_S01_E01_FRM-00-19-16-19
            # split attrs['img_frame'], keep attrs columns ['season_code', 'episode_code'], ignore others with _
            (_, attrs['season_code'],  attrs['episode_code'], _) = attrs['img_frame'].split("_")
            
            # define attrs['episode_id']
            attrs['episode_id'] = attrs['season_code'] + attrs['episode_code']
            
            self.attrs = attrs
              
        elif other_attrs is not None:
            '''
            use other_attrs to set own attrs
            '''
            self.attrs = other_attrs.copy()

        else:
            raise Exception("constructor requires s3s_line:str or  other_attrs:dict) argument")
    
    # object function
    def get_attrs(self) -> dict:
        return self.attrs
    
    @staticmethod
    def get_columns():
        return ['date', 'time', 'size', 'key', 'folder', 'img_class', 'img_frame', 'season_code', 'episode_code', 'episode_id']

    @staticmethod
    def split_s3_ls_line_of_df(df: pd.DataFrame) -> pd.DataFrame:
        # take the s3_ls_line column of df and split it out into all s3_episode_key columns - see get_columns() above
        
        # e.g. df.s3_ls_line = "2022-05-03 19:15:44       2336 tuttle_twins/s01e01/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"
        
        df.s3_ls_line = df.s3_ls_line.replace(r'\s+', ' ', regex=True)
        
        # e.g. df.s3_ls_line = "2022-05-03 19:15:44 2336 tuttle_twins/s01e01/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"
        
        obj_cols = ['date', 'time', 'size', 'key']
        obj_df = df.s3_ls_line.str.split(' ', expand=True).rename(columns = lambda x: obj_cols[x])
        df = pd.concat([df,obj_df], axis=1)
        df = df.drop(columns=['s3_ls_line'], axis=1, inplace=True)
        
        # e.g. df.date = "2022-05-03"
        # e.g. df.time = "19:15:44"
        # e.g. df.size = "2336"
        # e.g. df.key = "tuttle_twins/s01e01/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"
        
        key_cols = ['tt','se','ml','folder','img_class','img_frame','ext']
        key_df = df.key.str.split('/|\.', expand=True).rename(columns = lambda x: key_cols[x])
        key_df = key_df.drop(columns=['tt','se','ml','ext'], axis=1, inplace=True)
        df = pd.concat([df,key_df], axis=1)

        # e.g. df.folder = "validate"
        # e.g. df.img_class = "Uncommon"
        # e.g. df.img_frame = "TT_S01_E01_FRM-00-19-16-19"
        
        img_frame_cols = ['tt', 'season_code', 'episode_code', 'remainder']
        img_frame_df = df.img_frame.str.split('_', expand=True).rename(columns = lambda x: img_frame_cols[x])
        img_frame_df = img_frame_df.drop(columns=['tt','remainder'], axis=1, inplace=True)
        
        # e.g. df.season_code = "S01"
        # e.g. df.episode_code = "E01"
        
        img_frame_df['episode_id'] = img_frame_df.season_code + img_frame_df.episode_code
        # e.g. df.episode_id = "S01E01"

        df = pd.concat([df,img_frame_df], axis=1)   

        return df
    
    @staticmethod
    def split_key_in_df(df: pd.DataFrame) -> pd.DataFrame:
        # e.g. df.key = "tuttle_twins/s01e01/ML/validate/Uncommon/TT_S01_E01_FRM-00-19-16-19.jpg"
        
        key_cols = ['tt','se','ml','folder','img_class','img_frame','ext']
        key_df = df.key.str.split('/|\.', expand=True).rename(columns = lambda x: key_cols[x])
        key_df = key_df.drop(columns=['tt','se','ml','ext'], axis=1, inplace=True)
        df = pd.concat([df,key_df], axis=1)

        # e.g. df.folder = "validate"
        # e.g. df.img_class = "Uncommon"
        # e.g. df.img_frame = "TT_S01_E01_FRM-00-19-16-19"
        
        img_frame_cols = ['tt', 'season_code', 'episode_code', 'remainder']
        img_frame_df = df.img_frame.str.split('_', expand=True).rename(columns = lambda x: img_frame_cols[x])
        img_frame_df = img_frame_df.drop(columns=['tt','remainder'], axis=1, inplace=True)
        
        # e.g. df.season_code = "S01"
        # e.g. df.episode_code = "E01"
        
        img_frame_df['episode_id'] = img_frame_df.season_code + img_frame_df.episode_code
        # e.g. df.episode_id = "S01E01"

        df = pd.concat([df,img_frame_df], axis=1)   

        return df

################################
#  Tests
################################

import string
import random
    
def rstr(N=4):
    return ''.join(random.choice(string.ascii_letters) for i in range(N))
def rnum(N=4):
    return ''.join(random.choice(string.digits) for i in range(N))

def assert_equals(name, expected, result):
    assert result == expected, f"ERROR: expected {name}: {expected} not {result}"

def create_random_attrs():
    attrs = {}

    dt = rnum(8)
    tm = rnum(8)
    sz = rnum(4)
    tt = rstr(12)
    folder =  rstr(8)
    img_class = rstr(8)
    sc = 'S' + rnum(2)
    ec = 'E' + rnum(2)
    n1 = rnum(2)
    n2 = rnum(2)
    n3 = rnum(2)
    n4 = rnum(2)

    ifrm = 'TT_' + sc + '_' + ec + '_FRM' +  '-' + n1 + '-' + n2 + '-' + n3 + '-' + n4
    key =  tt + '/' + sc + ec + "/ML/" + folder + '/' + img_class + '/' + ifrm + ".jpg"
    s3_ls_line = dt + " " + tm + "     " + sz + " " + key
    
    attrs["s3_ls_line"] = s3_ls_line

    attrs['date'] = dt
    attrs["time"] = tm
    attrs["size"] = sz
    attrs["key"] = key
    
    attrs["folder"] = folder
    attrs["img_class"] = img_class
    attrs["img_frame"] = ifrm
    
    attrs["season_code"] = sc
    attrs["episode_code"] = ec
    attrs["episode_id"] = sc + ec
    
    return attrs

def create_random_s3_ls_line():
    attrs = create_random_attrs()
    return attrs['s3_ls_line']

def test_split_s3_ls_line_of_df():
    s3_ls_line = create_random_s3_ls_line()
    other_val = rstr(5)
    df = pd.DataFrame(columns=["s3_ls_line", "other_col"])
    df.loc[0] = [s3_ls_line, other_val]

    df = s3_episode_key.split_s3_ls_line_of_df(df)

    cols = s3_episode_key.get_columns()
    cols.extend(['other_col'])
    expected = set(cols)
    result = set(df.columns)
    assert result == expected, "ERROR: expected df.columns: {expected} not {result}"

def test_construct_from_s3_ls_line():
    rnd_attrs = create_random_attrs()
    s3_ls_line = rnd_attrs['s3_ls_line']
    
    obj = s3_episode_key(s3_ls_line=s3_ls_line)

    for i, (k,v) in enumerate(rnd_attrs.items()):
        assert_equals(k, v, obj.attrs[k])

def test_call_func_N_times(func, N):
    print(f"starting {N} calls of {func.__name__} ...")
    t1 = perf_counter()
    for i in range(1,N):
        func()
    elapsed_secs = perf_counter() - t1
    calls_per_sec = N / elapsed_secs
    secs_per_call = elapsed_secs / N
    print(f"... finished {N} calls of {func.__name__} in {elapsed_secs} secs, calls/sec:{calls_per_sec}  secs/call:{secs_per_call} ")

if __name__ == '__main__':
    test_construct_from_s3_ls_line()
    test_split_s3_ls_line_of_df()
    
    test_call_func_N_times(test_construct_from_s3_ls_line, 1000)
    test_call_func_N_times(test_split_s3_ls_line_of_df, 1000)
    
