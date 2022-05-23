# see https://stackoverflow.com/a/35805441/18218031

import os
import time
from typing import List
import random

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


# =============================================
# TESTS
# =============================================

def test_concatonate_files():
    letters = [chr(ord('a') + i) for i in range(26)]
    letters.extend(['\n','\t',' '])
    test_src_files = []
    total_chars = 0

    for i in range(3):
        time.sleep(0.001)
        test_src_file = f"/tmp/test-src-file-{round(time.time() * 1000)}"
        with open(test_src_file, "w") as f:
            c = random.choices(letters, k=100)
            s = "".join(c) + "\n"
            f.write(s)
        total_chars += len(s)
        test_src_files.append(test_src_file)

    test_dst_file = f"/tmp/test-dst-file-{round(time.time() * 1000)}"

    # here's the test
    concatonate_files(src_files=test_src_files, dst_file=test_dst_file)

    file_size = os.path.getsize(test_dst_file)
    assert file_size == total_chars

    for test_src_file in test_src_files:
        os.remove(test_src_file)

    os.remove(test_dst_file)


def test_concatonate_file():
    letters = [chr(ord('a') + i) for i in range(26)]
    letters.extend(['\n','\t',' '])
    total_chars = 0

    test_src_file = f"/tmp/test-src-file-{round(time.time() * 1000)}"
    with open(test_src_file, "w") as f:
        c = random.choices(letters, k=100)
        s = "".join(c) + "\n"
        f.write(s)
    total_chars += len(s)

    test_dst_file = f"/tmp/test-dst-file-{round(time.time() * 1000)}"

    # here's the test
    concatonate_file(src_file=test_src_file, dst_file=test_dst_file)

    file_size = os.path.getsize(test_dst_file)
    assert file_size == total_chars

    os.remove(test_src_file)
    os.remove(test_dst_file)

def test_generate_and_compare_100_Mbyte_bin_files():
    import shutil
    
    size_Mbytes = 100
    size_bytes = round(size_Mbytes * 1024*1024)
    tmp_file_1 = f"/tmp/tmp-file1-{round(time.time() * 1000)}"
    tmp_file_2 = f"/tmp/tmp-file2-{round(time.time() * 1000)}"

    generate_big_random_bin_file(filename=tmp_file_1,size=size_bytes)
    shutil.copy(tmp_file_1, tmp_file_2)
    assert compare_big_bin_files(tmp_file_1, tmp_file_2) == True

    generate_big_random_bin_file(filename=tmp_file_2,size=size_bytes)
    assert compare_big_bin_files(tmp_file_1, tmp_file_2) == False
    
    os.remove(tmp_file_1)
    os.remove(tmp_file_2)

    
if __name__ == "__main__":
    from logger_utils import set_all_info_loggers_to_debug_level
    set_all_info_loggers_to_debug_level()
    test_concatonate_files()
    test_concatonate_file()
    test_generate_and_compare_100_Mbyte_bin_files()
