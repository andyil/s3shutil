import unittest
import s3shutil
from unittests import s3testhelp
import boto3
import tempfile
import shutil
import deepdiff
import os
import random
import logging
import sys

logging.getLogger().setLevel(logging.WARN)
logging.getLogger('s3shutil').setLevel(logging.INFO)
logging.getLogger('s3testhelp').setLevel(logging.INFO)
logging.getLogger('tests').setLevel(logging.INFO)

logging.basicConfig(stream=sys.stdout, format='%(levelname)s:[%(threadName)s]:%(name)s:%(message)s')


caller = boto3.client('sts').get_caller_identity()

class TestS3Shutil(unittest.TestCase):

    def setUp(self):
        self.s3th = s3testhelp.S3TestHelp()
        self.fsroot1 = tempfile.mkdtemp(suffix='-111111', prefix='s3s-111111-')
        self.fsroot2 = tempfile.mkdtemp(suffix='-222222', prefix='s3s-222222-')
        self.s3root1 = self.s3th.get_tests_root()
        self.s3root2 = self.s3th.get_tests_root()

        self.log = logging.getLogger('tests')
        self.log.info('RUNNING TEST %s', self)
        self.log.info('s3root1 %s', self.s3root1)
        self.log.info('s3root2 %s', self.s3root2)

        self.maxDiff = None


    
    def assertObjEq(self, o1, o2, msg=None):
        jsondiff = deepdiff.DeepDiff(o1, o2).to_json()
        self.assertEqual(jsondiff, '{}', msg)

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
            fp = os.path.join(has_files, d)
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
        s3shutil.tree_sync(self.fsroot1, self.s3root1)

        j1 = self.s3th.s3_root_to_json(self.s3root1)
        j2 = self.s3th.fs_root_to_json(self.fsroot1)

        self.assertObjEq(j2, j1)

    def test_local_sync_to_s3_only_upload(self):
        self.populate1()
        s3shutil.copytree(self.fsroot1, self.s3root1)

        p = f'{self.s3root1}d3/d5/x'
        s3shutil.rmtree(p)

        s3shutil.tree_sync(self.fsroot1, self.s3root1)

        j1 = self.s3th.s3_root_to_json(self.s3root1)
        j2 = self.s3th.fs_root_to_json(self.fsroot1)

        self.assertObjEq(j2, j1)

    def test_local_sync_to_s3_only_delete(self):
        self.populate1()
        s3shutil.copytree(self.fsroot1, self.s3root1)

        delete_locally = os.path.join(self.fsroot1, 'd2', 'y')
        os.unlink(delete_locally)

        s3shutil.tree_sync(self.fsroot1, self.s3root1)

        j1 = self.s3th.fs_root_to_json(self.fsroot1)
        j2 = self.s3th.s3_root_to_json(self.s3root1)

        l1 = list(map(lambda x: x['Key'], j1))
        l2 = list(map(lambda x: x['Key'], j2))

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

        creation = self.fsroot1
        s3_src = self.s3root1
        fs_dest = self.fsroot2

        s3shutil.copytree(creation, s3_src)
        s3shutil.move(s3_src, fs_dest)

        creation_list = self.s3th.fs_root_to_json(creation)
        src_list = self.s3th.s3_root_to_json(s3_src)
        dest_list = self.s3th.fs_root_to_json(fs_dest)

        self.assertObjEq(src_list, [], 'move src in s3 should be empty')

        self.assertObjEq(creation_list, dest_list, 'move dest should be same as creation')

    def test_move_s3_to_s3(self):
        self.populate1()
        j0 = self.s3th.fs_root_to_json(self.fsroot1)        

        s3shutil.copytree(self.fsroot1, self.s3root1)

        s3shutil.move(self.s3root1, self.s3root2)
        
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        j2 = self.s3th.s3_root_to_json(self.s3root2)

        self.assertObjEq(j1, [])

        self.assertObjEq(j0, j2)

    def test_copyfile_up(self):
        path = os.path.join(self.fsroot1, 'jhello.txt')
        self.write(path)

        dst_name = 'destination.txt'

        dst = f'{self.s3root1}{dst_name}'
        s3shutil.copyfile(path, dst)

        j1 = self.s3th.fs_root_to_json(self.fsroot1)
        j2 = self.s3th.s3_root_to_json(self.s3root1)

        j1[0]['Key'] = dst_name

        print(j1)
        print(j2)
        self.assertObjEq(j1, j2)

    def test_copyfile_down(self):
        path = os.path.join(self.fsroot1, 'jhello.txt')
        self.write(path)

        dst_name1 = 'destination.txt'
        dst_name2 = 'destination-c.txt'

        dst = f'{self.s3root1}{dst_name1}'
        s3shutil.copyfile(path, dst)

        dst2 = os.path.join(self.fsroot2, dst_name2)

        s3shutil.copyfile(dst, dst2)

        j1 = self.s3th.fs_root_to_json(self.fsroot1)
        j2 = self.s3th.fs_root_to_json(self.fsroot2)


        print(j1)
        print(j2)
        j1[0]['Key'] = dst_name2


        self.assertObjEq(j1, j2)


    def test_copyfile_s3_to_s3(self):
        path = os.path.join(self.fsroot1, 'jhello.txt')
        self.write(path)

        dst_name1 = 'destination.txt'
        dst_name2 = 'destination-c.txt'

        dst = f'{self.s3root1}{dst_name1}'

        self.log.info('copyfile %s %s', path, dst)
        s3shutil.copyfile(path, dst)

        dst2 = f'{self.s3root2}{dst_name2}'

        self.log.info('copyfile %s %s', dst, dst2)
        s3shutil.copyfile(dst, dst2)

        self.log.info('Checking s3root1 %s', self.s3root1)
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        self.log.info(f'j1 {j1}')

        self.log.info('Checking s3root2 %s', self.s3root2)
        j2 = self.s3th.s3_root_to_json(self.s3root2)
        self.log.info(f'j2 {j2}')

        j1[0]['Key'] = dst_name2

        self.assertObjEq(j1, j2)

    def test_copy_up_to_file(self):
        path = os.path.join(self.fsroot1, 'jhello.txt')
        self.write(path)

        dst_name = 'destination.txt'

        dst = f'{self.s3root1}{dst_name}'
        s3shutil.copy(path, dst)

        j1 = self.s3th.fs_root_to_json(self.fsroot1)
        j2 = self.s3th.s3_root_to_json(self.s3root1)

        j1[0]['Key'] = dst_name

        print(j1)
        print(j2)
        self.assertObjEq(j1, j2)

    def test_copy_down_to_file(self):
        path = os.path.join(self.fsroot1, 'jhello.txt')
        self.write(path)

        dst_name1 = 'destination.txt'
        dst_name2 = 'destination-c.txt'

        dst = f'{self.s3root1}{dst_name1}'
        s3shutil.copy(path, dst)

        dst2 = os.path.join(self.fsroot2, dst_name2)

        s3shutil.copy(dst, dst2)

        j1 = self.s3th.fs_root_to_json(self.fsroot1)
        j2 = self.s3th.fs_root_to_json(self.fsroot2)


        print(j1)
        print(j2)
        j1[0]['Key'] = dst_name2


        self.assertObjEq(j1, j2)


    def test_copy_s3_to_s3_to_file(self):
        path = os.path.join(self.fsroot1, 'jhello.txt')
        self.write(path)

        dst_name1 = 'destination.txt'
        dst_name2 = 'destination-c.txt'

        dst = f'{self.s3root1}{dst_name1}'

        self.log.info('copyfile %s %s', path, dst)
        s3shutil.copy(path, dst)

        dst2 = f'{self.s3root2}{dst_name2}'

        self.log.info('copyfile %s %s', dst, dst2)
        s3shutil.copy(dst, dst2)

        self.log.info('Checking s3root1 %s', self.s3root1)
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        self.log.info(f'j1 {j1}')

        self.log.info('Checking s3root2 %s', self.s3root2)
        j2 = self.s3th.s3_root_to_json(self.s3root2)
        self.log.info(f'j2 {j2}')

        j1[0]['Key'] = dst_name2

        self.assertObjEq(j1, j2)


    def test_copy_up_to_dir(self):
        n = 'jhello.txt'
        path = os.path.join(self.fsroot1, n)
        self.write(path)

        dst_name = 'destination'

        dst = f'{self.s3root1}{dst_name}/'
        s3shutil.copy(path, dst)

        j1 = self.s3th.fs_root_to_json(self.fsroot1)
        j2 = self.s3th.s3_root_to_json(self.s3root1)

        j1[0]['Key'] = f'{dst_name}/{n}'

        print(j1)
        print(j2)
        self.assertObjEq(j1, j2)

    def test_copy_down_to_dir(self):
        path = os.path.join(self.fsroot1, 'jhello.txt')
        self.write(path)

        dst_name1 = 'destination.txt'
        dst_name2 = 'destination-dir'

        dst = f'{self.s3root1}{dst_name1}'
        s3shutil.copy(path, dst)

        dst2 = os.path.join(self.fsroot2, dst_name2)

        s3shutil.copy(dst, dst2)

        j1 = self.s3th.fs_root_to_json(self.fsroot1)
        j2 = self.s3th.fs_root_to_json(self.fsroot2)


        print(j1)
        print(j2)
        j1[0]['Key'] = dst_name2

        self.assertObjEq(j1, j2)


    def test_copy_s3_to_s3_to_dir(self):
        path = os.path.join(self.fsroot1, 'jhello.txt')
        self.write(path)

        dst_name1 = 'destination.txt'
        dst_name2 = 'destination-folder'

        dst = f'{self.s3root1}{dst_name1}'

        self.log.info('copyfile 1 %s %s', path, dst)
        s3shutil.copyfile(path, dst)

        dst2 = f'{self.s3root2}{dst_name2}/'

        self.log.info('copy 2 %s %s', dst, dst2)
        s3shutil.copy(dst, dst2)

        self.log.info('Checking s3root1 %s', self.s3root1)
        j1 = self.s3th.s3_root_to_json(self.s3root1)
        self.log.info(f'j1 {j1}')

        self.log.info('Checking s3root2 %s', self.s3root2)
        j2 = self.s3th.s3_root_to_json(self.s3root2)
        self.log.info(f'j2 {j2}')

        j1[0]['Key'] = 'destination-folder/destination.txt'

        self.assertObjEq(j1, j2)



if __name__ == '__main__':
    unittest.main()
