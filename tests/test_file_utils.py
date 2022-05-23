# call from project directory
# python -m unittest tests/test_file_utils.py

import unittest

from file_utils import *

class TestFileUtilMethods(unittest.TestCase):


    def test_concatonate_files(self):
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
        self.assertEqual(file_size, total_chars)

        for test_src_file in test_src_files:
            os.remove(test_src_file)

        os.remove(test_dst_file)


    def test_concatonate_file(self):
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
        self.assertEqual(file_size, total_chars)

        os.remove(test_src_file)
        os.remove(test_dst_file)
        

    def test_generate_and_compare_100_Mbyte_bin_files(self):
        import shutil
        
        size_Mbytes = 100
        size_bytes = round(size_Mbytes * 1024*1024)
        tmp_file_1 = f"/tmp/tmp-file1-{round(time.time() * 1000)}"
        tmp_file_2 = f"/tmp/tmp-file2-{round(time.time() * 1000)}"

        generate_big_random_bin_file(filename=tmp_file_1,size=size_bytes)
        shutil.copy(tmp_file_1, tmp_file_2)
        self.assertTrue(compare_big_bin_files(tmp_file_1, tmp_file_2))

        generate_big_random_bin_file(filename=tmp_file_2,size=size_bytes)
        self.assertFalse( compare_big_bin_files(tmp_file_1, tmp_file_2))
        
        os.remove(tmp_file_1)
        os.remove(tmp_file_2)

    
if __name__ == '__main__':
    unittest.main()
