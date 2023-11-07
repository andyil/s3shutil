import unittest
import s3shutil
from unittests import s3testhelp
import boto3
import tempfile
import shutil



class TestStringMethods(unittest.TestCase):

    def setUp(self):
        self.s3th = s3testhelp.S3TestHelp()
        self.fsroot1 = tempfile.mkdtemp()
        self.fsroot2 = tempfile.mkdtemp()
        self.s3root1 = self.s3th.get_tests_root()
        self.s3root2 = self.s3th.get_tests_root()
        print(f'tempdir {self.fsroot1}')
        print(f'tempdir {self.fsroot2}')
        print(f'tempdir {self.s3root1}')
        print(f'tempdir {self.s3root2}')


    def tearDown(self):        
        print()
        shutil.rmtree(self.fsroot1)
        shutil.rmtree(self.fsroot2)
        print(f'Removed {self.fsroot1}')
        print(f'Removed {self.fsroot1}')

    def test1(self):
        open(f'{self.fsroot1}/a.txt', 'w').write('hello')
        s3shutil.copytree(self.fsroot1, self.s3root1)
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        j2 = self.s3th.fs_root_to_json(self.fsroot1)
        print(j1)
        print(j2)

    def test2(self):
        open(f'{self.fsroot1}/a.txt', 'w').write('hello')
        open(f'{self.fsroot1}/c.txt', 'w').write('dadsasd')
        s3shutil.copytree(self.fsroot1, self.s3root1)
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        j2 = self.s3th.fs_root_to_json(self.fsroot1)
        print(j1)
        print(j2)

    def test3(self):
        open(f'{self.fsroot1}/c.txt', 'w').write('da1dsasd')
        open(f'{self.fsroot1}/a.txt', 'w').write('sdf')
        open(f'{self.fsroot1}/b.txt', 'w').write('dadsasfdsdsd')
        s3shutil.copytree(self.fsroot1, self.s3root1)
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        j2 = self.s3th.fs_root_to_json(self.fsroot1)
        print(j1)
        print(j2)

    def test_back(self):
        open(f'{self.fsroot1}/c.txt', 'w').write('da1dsasd')
        open(f'{self.fsroot1}/a.txt', 'w').write('sdf')
        open(f'{self.fsroot1}/b.txt', 'w').write('dadsasfdsdsd')
        s3shutil.copytree(self.fsroot1, self.s3root1)
        s3shutil.copytree(self.s3root1, self.fsroot2)
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        j2 = self.s3th.fs_root_to_json(self.fsroot1)
        j3 = self.s3th.fs_root_to_json(self.fsroot2)
        print(j1)
        print(j2)
        print(j3)

   

if __name__ == '__main__':
    unittest.main()