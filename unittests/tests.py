import unittest
import s3shutil
from unittests import s3testhelp
import boto3
import tempfile
import shutil
import deepdiff
import os
import random

print('Get caller identity')
caller = boto3.client('sts').get_caller_identity()
print(caller)

class TestS3Shutil(unittest.TestCase):

    def setUp(self):
        self.s3th = s3testhelp.S3TestHelp()
        self.fsroot1 = tempfile.mkdtemp()
        self.fsroot2 = tempfile.mkdtemp()
        self.s3root1 = self.s3th.get_tests_root()
        self.s3root2 = self.s3th.get_tests_root()

        self.maxDiff = None
        
    
    def assertObjEq(self, o1, o2):
        jsondiff = deepdiff.DeepDiff(o1, o2).to_json()
        self.assertEqual(jsondiff, '{}')

    def write(self, path, body=None):
        if body is None:
            length = random.randrange(1_000, 10_000)
            body = random.randbytes(length)
        if type(body) == str:
            mode = 'w'
        else:
            mode = 'wb'
        with open(path, mode) as f:
            f.write(body)

    def tearDown(self):                
        shutil.rmtree(self.fsroot1)
        shutil.rmtree(self.fsroot2)       
        s3shutil.rmtree(self.s3root1)
        s3shutil.rmtree(self.s3root2)

    def populate1(self):
        root_dir = self.fsroot1
        self.write(f'{root_dir}/c.txt')
        self.write(f'{root_dir}/a.txt')
        self.write(f'{root_dir}/b.txt')
        empty = os.path.join(root_dir, 'd1')
        has_files = os.path.join(root_dir, 'd2')

        os.mkdir(empty)
        os.mkdir(has_files)

        for d in 'x', 'y', 'z':
            fp = os.path.join(has_files, 'd')
            self.write(fp)


        has_only_dirs = os.path.join(root_dir, 'd3')
        os.mkdir(has_only_dirs)

        d4 = os.path.join(has_only_dirs, 'd4')
        d5 = os.path.join(has_only_dirs, 'd5')

        os.mkdir(d4)
        os.mkdir(d5)

        for f in 'x', 'y', 'z':
            for d in d4, d5:                
                fp = os.path.join(d, f)
                self.write(fp)
        

    def test_one_file(self):
        self.write(f'{self.fsroot1}/a.txt', 'hello')
        s3shutil.copytree(self.fsroot1, self.s3root1)
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        j2 = self.s3th.fs_root_to_json(self.fsroot1)
        self.assertObjEq(j1, j2)

    def test_two_files(self):
        self.write(f'{self.fsroot1}/a.txt', 'hello')
        self.write(f'{self.fsroot1}/c.txt', 'dadsasd')
        s3shutil.copytree(self.fsroot1, self.s3root1)
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        j2 = self.s3th.fs_root_to_json(self.fsroot1)
        self.assertObjEq(j1, j2)

    def test_empty(self):    
        s3shutil.copytree(self.fsroot1, self.s3root1)
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        j2 = self.s3th.fs_root_to_json(self.fsroot1)
        self.assertObjEq(j1, j2)


    def test_s3_to_fs(self):
        self.populate1()      
        s3shutil.copytree(self.fsroot1, self.s3root1)
        s3shutil.copytree(self.s3root1, self.fsroot2)
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        j2 = self.s3th.fs_root_to_json(self.fsroot1)
        j3 = self.s3th.fs_root_to_json(self.fsroot2)
        self.assertObjEq(j1, j2)
        self.assertObjEq(j1, j3)

    def test_local_sync_to_s3_no_changes(self):
        self.populate1()      
        s3shutil.copytree(self.fsroot1, self.s3root1)

        from s3shutil.s3shutil import S3ShutilEngine
        eng = S3ShutilEngine(None, None)
        
        #os.unlink(os.path.join(self.fsroot1, 'd2', 'd'))
        p = f'{self.s3root1}d3/d5/x'
        print(f'Deleting {p}')
        #s3shutil.rmtree(p)

        eng.execute_upload_sync(self.fsroot1, self.s3root1)

        j1 = self.s3th.s3_root_to_json(self.s3root1)
        j2 = self.s3th.fs_root_to_json(self.fsroot1)

        self.assertObjEq(j2, j1)

    def test_local_sync_to_s3_only_upload(self):
        self.populate1()
        s3shutil.copytree(self.fsroot1, self.s3root1)

        from s3shutil.s3shutil import S3ShutilEngine
        eng = S3ShutilEngine(None, None)

        p = f'{self.s3root1}d3/d5/x'
        print(f'Del {p} for test')
        s3shutil.rmtree(p)

        eng.execute_upload_sync(self.fsroot1, self.s3root1)

        j1 = self.s3th.s3_root_to_json(self.s3root1)
        j2 = self.s3th.fs_root_to_json(self.fsroot1)

        self.assertObjEq(j2, j1)

    def test_local_sync_to_s3_only_delete(self):
        self.populate1()
        s3shutil.copytree(self.fsroot1, self.s3root1)

        from s3shutil.s3shutil import S3ShutilEngine
        eng = S3ShutilEngine(None, None)

        delete_locally = os.path.join(self.fsroot1, 'd2', 'd')
        print(f'Deleting {delete_locally}')
        print(f'Exists {os.path.exists(delete_locally)}')
        os.unlink(delete_locally)
        #s3shutil.rmtree(p)
        print('Deleted')
        print(f'Exists {os.path.exists(delete_locally)}')

        eng.execute_upload_sync(self.fsroot1, self.s3root1)

        j1 = self.s3th.fs_root_to_json(self.fsroot1)
        j2 = self.s3th.s3_root_to_json(self.s3root1)

        l1 = list(map(lambda x: x['Key'], j1))
        l2 = list(map(lambda x: x['Key'], j2))

        print('local')
        print(list(l1))
        print('remote')
        print(list(l2))

        self.assertObjEq(l1, l2)


    def test_rmtree(self):
        self.populate1()      
        s3shutil.copytree(self.fsroot1, self.s3root1)
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        s3shutil.rmtree(self.s3root1)
        j2 = self.s3th.s3_root_to_json(self.s3root1)
        self.assertObjEq(j2, [])

    def test_s3_to_s3(self):
        self.populate1()      
        s3shutil.copytree(self.fsroot1, self.s3root1)
        s3shutil.copytree(self.s3root1, self.s3root2)
        j1 = self.s3th.s3_root_to_json(self.s3root1)        
        j2 = self.s3th.s3_root_to_json(self.s3root2)
        self.assertObjEq(j2, j1)

    def test_move_fs_to_s3(self):
        self.populate1()      

        j0 = self.s3th.fs_root_to_json(self.fsroot1)        

        s3shutil.move(self.fsroot1, self.s3root1)
        j1 = self.s3th.s3_root_to_json(self.s3root1)        

        self.assertObjEq(j1, j0)

        #recreate self.fsroot1 so that teardown does not complain
        os.mkdir(self.fsroot1)

    def test_move_s3_to_fs(self):
        self.populate1()
        s3shutil.copytree(self.fsroot1, self.s3root1)
        s3shutil.move(self.s3root1, self.fsroot2)

        j0 = self.s3th.fs_root_to_json(self.fsroot1)        
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        j2 = self.s3th.fs_root_to_json(self.fsroot2)

        self.assertObjEq(j1, [])

        self.assertObjEq(j0, j2)

    def test_move_s3_to_s3(self):
        self.populate1()
        j0 = self.s3th.fs_root_to_json(self.fsroot1)        

        s3shutil.copytree(self.fsroot1, self.s3root1)

        s3shutil.move(self.s3root1, self.s3root2)
        
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        j2 = self.s3th.s3_root_to_json(self.s3root2)

        self.assertObjEq(j1, [])

        self.assertObjEq(j0, j2)

        

   

if __name__ == '__main__':
    unittest.main()
