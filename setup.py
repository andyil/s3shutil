from distutils.core import setup
setup(
  name = 's3shutil',
  packages = ['s3shutil'],
  version = '0.27',
  license='MIT',
  description = 'A shutil like interface to AWS S3',
  url = 'https://github.com/18ijq3l793/s3shutil',
  download_url = 'https://github.com/18ijq3l793/s3shutil/archive/0.27.tar.gz',
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
  ],
)