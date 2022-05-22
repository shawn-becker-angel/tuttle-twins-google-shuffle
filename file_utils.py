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
        print("wrote test_src_file:", test_src_file)
        test_src_files.append(test_src_file)

    test_dst_file = f"/tmp/test-dst-file-{round(time.time() * 1000)}"

    # here's the test
    concatonate_files(src_files=test_src_files, dst_file=test_dst_file)

    print("filesize test_dst_file:", test_dst_file)
    file_size = os.path.getsize(test_dst_file)
    assert file_size == total_chars

    for test_src_file in test_src_files:
        os.remove(test_src_file)
        print("removed test_src_file:", test_src_file)

    os.remove(test_dst_file)
    print("removed test_dst_file:", test_dst_file)


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
    print("wrote test_src_file:", test_src_file)

    test_dst_file = f"/tmp/test-dst-file-{round(time.time() * 1000)}"

    # here's the test
    concatonate_file(src_file=test_src_file, dst_file=test_dst_file)

    print("filesize test_dst_file:", test_dst_file)
    file_size = os.path.getsize(test_dst_file)
    assert file_size == total_chars

    os.remove(test_src_file)
    print("removed test_src_file:", test_src_file)

    os.remove(test_dst_file)
    print("removed test_dst_file:", test_dst_file)


if __name__ == "__main__":
    from logger_utils import set_all_info_loggers_to_debug_level
    set_all_info_loggers_to_debug_level()
    test_concatonate_files()
    test_concatonate_file()