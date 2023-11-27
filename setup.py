from distutils.core import setup

setup(
  name = 's3shutil',
  packages = ['s3shutil'],

  long_description=open('README.rst').read(),
  long_description_content_type='text/x-rst',
  version = '0.35',
  license='MIT',
  description = 'A shutil like interface to AWS S3',
  url = 'https://github.com/andyil/s3shutil',
  download_url = 'https://github.com/andyil/s3shutil/archive/0.28.tar.gz',
  keywords = ['aws', 's3', 'cloud', 'storage', 'shutil', 'network'],
  install_requires=['boto3'],
  classifiers=[
    'Development Status :: 3 - Alpha',
    'Intended Audience :: Developers',
    'Intended Audience :: Information Technology',
    'Intended Audience :: System Administrators',
    'Topic :: Internet',
    'License :: OSI Approved :: MIT License',
    'Programming Language :: Python :: 3',
    'Programming Language :: Python :: 3.4',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6',
    'Programming Language :: Python :: 3.8',
    'Programming Language :: Python :: 3.9',
    'Programming Language :: Python :: 3.10',
    'Programming Language :: Python :: 3.11',
    'Programming Language :: Python :: 3.12',
  ],
)