import unittest
import s3shutil
from unittests import s3testhelp
import tempfile
import shutil
import deepdiff
import os
import random
import logging
import sys

eng = logging.getLogger('eng')
logging.getLogger().setLevel(logging.WARN)
eng.setLevel(logging.INFO)
logging.basicConfig(stream=sys.stdout, format='%(levelname)s:%(threadName)s:%(message)s')

class ShortTestS3Shutil(unittest.TestCase):

    def setUp(self):
        print('setup')
        
    

    def tearDown(self):
        print('teardown')


        

    def testA(self):
        print('test-a')

    def testB(self):
        print('test-b')



        

   

if __name__ == '__main__':
    unittest.main()
