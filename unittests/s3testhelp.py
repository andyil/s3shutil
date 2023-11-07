import boto3
import secrets
import datetime
import logging
import os



class S3TestHelp:

    def __init__(self):
        self.prefix = 's3shutil-test-bucket-'
        self.client = None
        self.region_name = None

    def get_client(self):
        if self.client is None:
            session = boto3.Session()
            self.region_name = session.region_name
            self.client = session.client('s3')

        return self.client

    def get_tests_bucket(self):
        s3 = self.get_client()
        r = s3.list_buckets()
        for bucket in r['Buckets']:
            name = bucket['Name']
            if name.startswith(self.prefix):
                return name

        rand = secrets.token_urlsafe(8)
        name = f'{self.prefix}{rand}'.lower()
        print(name)
        s3.create_bucket(Bucket=name, CreateBucketConfiguration={'LocationConstraint': self.region_name})

        return name

    def get_tests_root(self):
        bucket, prefix = self.get_tests_root_as_tuple()
        return f's3://{bucket}/{prefix}/'


    def get_tests_root_as_tuple(self):
        now = datetime.datetime.now()
        nowpart = now.strftime('%Y%m%d-%H%M%S')
        randpart = secrets.token_urlsafe(5)
        bucket = self.get_tests_bucket()
        prefix = f'{nowpart}-{randpart}'
        return bucket, prefix

    def s3_root_to_json(self, root):
        s3 = self.get_client()
        parts = root.split('/')        
        bucket = parts[2]
        prefix = '/'.join(parts[3:])
        result = []
        r = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        contents = r['Contents']
        for elem in contents:            
            key = elem['Key']
            r = s3.get_object(Bucket=bucket, Key=key, Range='bytes 0-100/*')
            text = r['Body'].read()
            rel = key.replace(prefix, '')
            entry = {'Key': rel, 'Size': elem['Size'], 'Body': text[:100]}
            result.append(entry)

        return result

    def fs_root_to_json(self, root):
        all_files = []
        for dir, dirs, files in os.walk(root):
            for file in files:
                print(file)
                fp = os.path.join(dir, file)
                rel = os.path.relpath(fp, root)

                uniform_sep = rel.replace(os.sep, '/')

                content = open(fp, 'rb').read()[:100]
                size = os.stat(fp).st_size

                entry = {'Key': uniform_sep, 'Size': size, 'Body': content}
                all_files.append(entry)
                
        all_files.sort(key=lambda x:x['Key'])
        return all_files        

            

if __name__=='__main__':
    sth = S3TestHelp()
    #sth.get_tests_root()
    #sth.s3_root_to_json('s3://tolaatmish/binaries/python/')
    sth.fs_root_to_json('C:\\Users\\andy\\github\\s3shutil')