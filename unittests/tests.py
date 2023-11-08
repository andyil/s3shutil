import unittest
import s3shutil
from unittests import s3testhelp
import boto3
import tempfile
import shutil
import deepdiff


class TestStringMethods(unittest.TestCase):

    def setUp(self):
        self.s3th = s3testhelp.S3TestHelp()
        self.fsroot1 = tempfile.mkdtemp()
        self.fsroot2 = tempfile.mkdtemp()
        self.s3root1 = self.s3th.get_tests_root()
        self.s3root2 = self.s3th.get_tests_root()
        
    
    def assertObjEq(self, o1, o2):
        jsondiff = deepdiff.DeepDiff(o1, o2).to_json()
        print(f'deepdiff {jsondiff}')
        self.assertEqual(jsondiff, '{}')

    def write(self, path, body):
        with open(path, 'w') as f:
            f.write(body)

    def tearDown(self):        
        print()
        shutil.rmtree(self.fsroot1)
        shutil.rmtree(self.fsroot2)        

    def test1(self):
        self.write(f'{self.fsroot1}/a.txt', 'hello')
        s3shutil.copytree(self.fsroot1, self.s3root1)
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        j2 = self.s3th.fs_root_to_json(self.fsroot1)
        self.assertObjEq(j1, j2)

    def test2(self):
        self.write(f'{self.fsroot1}/a.txt', 'hello')
        self.write(f'{self.fsroot1}/c.txt', 'dadsasd')
        s3shutil.copytree(self.fsroot1, self.s3root1)
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        j2 = self.s3th.fs_root_to_json(self.fsroot1)
        self.assertObjEq(j1, j2)

    def test3(self):
        self.write(f'{self.fsroot1}/c.txt', 'da1dsasd')
        self.write(f'{self.fsroot1}/a.txt', 'sdf')
        self.write(f'{self.fsroot1}/b.txt', 'dadsasfdsdsd')
        s3shutil.copytree(self.fsroot1, self.s3root1)
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        j2 = self.s3th.fs_root_to_json(self.fsroot1)
        self.assertObjEq(j1, j2)


    def test_back(self):
        self.write(f'{self.fsroot1}/c.txt', 'da1dsasd')
        self.write(f'{self.fsroot1}/a.txt', 'sfg')
        self.write(f'{self.fsroot1}/b.txt', 'dadsasfdsdsd')        
        s3shutil.copytree(self.fsroot1, self.s3root1)
        s3shutil.copytree(self.s3root1, self.fsroot2)
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        j2 = self.s3th.fs_root_to_json(self.fsroot1)
        j3 = self.s3th.fs_root_to_json(self.fsroot2)
        self.assertObjEq(j1, j2)
        self.assertObjEq(j1, j3)
       

    def test_rmtree(self):
        self.write(f'{self.fsroot1}/c.txt', 'da1dsasd')
        self.write(f'{self.fsroot1}/a.txt', 'sfg')
        self.write(f'{self.fsroot1}/b.txt', 'dadsasfdsdsd')    
        s3shutil.copytree(self.fsroot1, self.s3root1)
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        s3shutil.rmtree(self.s3root1)
        j2 = self.s3th.s3_root_to_json(self.s3root1)
        self.assertObjEq(j2, [])
   

if __name__ == '__main__':
    unittest.main()