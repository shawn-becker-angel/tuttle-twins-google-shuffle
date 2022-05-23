# see https://stackoverflow.com/a/35805441/18218031

from typing import List

import logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger("file_utils")


def concatonate_files(src_files: List[str], dst_file: str):
    '''Creates dst_file if needed'''
    with open (dst_file, "a") as dst:
        for src_file in src_files:
            with open (src_file, 'r') as src:
                for line in src:
                    dst.write(line)


def concatonate_file(src_file: str, dst_file: str):
    concatonate_files(src_files=[src_file], dst_file=dst_file)


def generate_big_random_bin_file(filename: str,size: int) -> None:
    """
    from https://www.bswen.com/2018/04/python-How-to-generate-random-large-file-using-python.html
    generate big binary file with the specified size in bytes
    :param filename: the filename
    :param size: the size in bytes
    :return:None
    """
    import os 
    with open('%s'%filename, 'wb') as fout:
        fout.write(os.urandom(size)) #1

def compare_big_bin_files(name1: str, name2: str) -> bool:
    '''
    Return True if the two large binary files are identical
    from https://www.quora.com/profile/Jon-Obermark-2
    '''
    with open(name1, "rb") as one: 
        with open(name2, "rb") as two: 
            chunk = other = True 
            while chunk or other: 
                chunk = one.read(1000) 
                other = two.read(1000) 
                if chunk != other: 
                    return False 
    return True 


if __name__ == "__main__":
    logger.info("done")
